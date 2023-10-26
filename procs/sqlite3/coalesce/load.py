import codecs
from datetime import datetime
import os
import procs.sqlite3.helper as helper
import hashlib
import uuid 
snowflake_external_table_surrogate_attributes = [['', 'VALUE', 'VARIANT'], ['','FILENAMEDATE', 'STRING'], ['','METADATA$FILE_ROW_NUMBER', 'NUMBER(38,0)']]
def create_uuid(input_string:str):

  m = hashlib.md5()
  m.update(input_string.encode('utf-8'))
  return str(uuid.UUID(m.hexdigest()))

def get_columns(cursor, source_table_name, landing_table_name, landing_table_id, target_table_id, source_type, config):
  command_tmp = ""
  with open(os.path.join(".","templates","coalesce", "load-column.yaml"),"r") as f:
    command_tmp = f.read()
  f.close()
  return_value = ""
  query = f"""SELECT 
                  source_attribute_name
                , data_type
                , format
                , selection
                , attribute_order
                , value
              FROM source_table_attributes
              WHERE source_table_name = '{source_table_name}'"""
  
  cursor.execute(query)
  columns = cursor.fetchall()

  for columns_row in columns:
    column_name = str(columns_row[0])
    column_dataType = str(columns_row[1])
    column_type = str(columns_row[3])
    column_order = str(columns_row[4])
    column_id = target_table_id + '__' + column_name
 
    column_description = '""'
    if column_type == 'columns' and source_type in ('snowflake_external_table_surrogate','snowflake_external_table' ):
      ext_json_column_template = str(config.get("landing", "EXT_JSON_COLUMN_NAME"))
      column_transform = ext_json_column_template.replace("@@landing_table_name", landing_table_name)
      column_transform = column_transform.replace("@@column_order", column_order)
      landing_table_attribute_id = landing_table_id + '__' + str('VALUE')
    elif column_type in ('additional_columns','default_columns'):
      landing_table_attribute_id = ''
      landing_table_id = ''
      column_transform = str(columns_row[5])
    else:
      column_transform = str(columns_row[5])
      landing_table_attribute_id = landing_table_id + '__' + column_name.upper()

    command = command_tmp.replace("@@column_uuid",create_uuid(column_id))
    command = command.replace("@@table_uuid",create_uuid(target_table_id))
    command = command.replace("@@column_name",column_name)
    command = command.replace("@@column_dataType",column_dataType)
    command = command.replace("@@column_description",column_description)    
    command = command.replace("@@source_column_uuid",create_uuid(landing_table_attribute_id))    
    command = command.replace("@@source_table_uuid",create_uuid(landing_table_id))    
    command = command.replace("@@transform",column_transform)    
    return_value = return_value + command
  return return_value

def generate_load(cursor, source_table_name, model_path, config):
  source_table_name = source_table_name.upper()
  query = f"""SELECT 
                source_type
              FROM source_tables
              WHERE source_table_name = '{source_table_name}'"""
  cursor.execute(query)
  tables = cursor.fetchall()
  table_id = ""
  table_name = ""
  source_type = ""

  for source_table_row in tables: 
        print(str(source_table_row[0]))
        source_type = str(source_table_row[0])

  if source_type != 'snowflake_external_table_surrogate':
    source_prefix = config.get("landing", "PREFIX")
  else:
    source_prefix = config.get("landing", "PREFIX_EXT")
  
  landing_location = config.get("landing", "LOCATION")
  landing_table_name = source_prefix + '_' + source_table_name.upper()
  landing_table_id = landing_location + '__' + landing_table_name

  load_node_prefix = config.get("load", "PREFIX")
  load_location = str(config.get("load", "LOCATION")).upper()
  load_table_name = load_node_prefix + '_' + source_table_name.upper()
  load_table_id = load_location + '__' + source_prefix + '__' + source_table_name.upper()
  load_materialization_type = str(config.get("load", "MATERIALIZATIONTYPE"))
  load_node_template_id = str(config.get("load", "NODE_TEMPLATE_ID")).upper()


  print('generate_load (step1): ' + load_table_name)
  model_path = model_path

  with open(os.path.join(".","templates","coalesce", "load.yaml"),"r") as f:
      command = f.read()
  f.close()
  columns = get_columns(cursor, source_table_name=source_table_name
                        ,landing_table_name= landing_table_name
                        , landing_table_id=landing_table_id
                        , target_table_id=load_table_id
                        , source_type=source_type
                        , config=config)

  command = command.replace("@@table_uuid", create_uuid(load_table_id))
  command = command.replace("@@table_name", load_table_name)
  command = command.replace("@@locationName", load_location)
  command = command.replace("@@materializationType", load_materialization_type)
  command = command.replace("@@node_template_id", load_node_template_id)
  command = command.replace("@@source_locationName", landing_location)
  command = command.replace("@@source_table_name", landing_table_name)
  command = command.replace("@@columns",columns)

  filename = os.path.join(model_path, f"{load_location}-{load_table_name.upper()}.yml")

  path =model_path

  # Check whether the specified path exists or not
  isExist = os.path.exists(path)
  if not isExist:   
  # Create a new directory because it does not exist 
      os.makedirs(path)

  with open(filename, 'w') as f:
    f.write(command.expandtabs(2))

  print(f"Created model \'{filename}\'")