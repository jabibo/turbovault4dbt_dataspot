import codecs
from datetime import datetime
import os
import procs.sqlite3.helper as helper
import hashlib
import uuid 
snowflake_external_table_surrogate_attributes = [['', 'VALUE', 'VARIANT'], ['','FILENAMEDATE', 'STRING'], ['','METADATA$FILE_ROW_NUMBER', 'STRING']]
def create_uuid(input_string:str):

  m = hashlib.md5()
  m.update(input_string.encode('utf-8'))
  return str(uuid.UUID(m.hexdigest()))

def get_source_attributes(cursor, source_table_name, target_table_id, source_type):
  command_tmp = ""
  with open(os.path.join(".","templates","coalesce", "landing-column.yaml"),"r") as f:
    command_tmp = f.read()
  f.close()
  return_value = ""
  if source_type != 'snowflake_external_table_surrogate':
    query = f"""SELECT 
                  source_table_attribute_id
                , source_attribute_name
                , data_type
                , format
              FROM source_table_attributes
              WHERE source_table_name = '{source_table_name}'"""
  
    cursor.execute(query)
    source_columns = cursor.fetchall()
  else: 
    source_columns = snowflake_external_table_surrogate_attributes

  for source_columns_row in source_columns:
    landing_table_attribute_id = target_table_id + '__' + str(source_columns_row[1])
    column_name = str(source_columns_row[1])
    column_dataType = str(source_columns_row[2])
    print(landing_table_attribute_id)
    command = command_tmp.replace("@@column_uuid",create_uuid(landing_table_attribute_id))
    command = command.replace("@@table_uuid",create_uuid(target_table_id))
    command = command.replace("@@column_name",column_name)
    command = command.replace("@@column_dataType",column_dataType)
    
    return_value = return_value + command
  return return_value

def generate_landing(cursor, source_table_name, model_path, config):
  source_table_name = source_table_name.upper()
  query = f"""SELECT 
                  source_type
              FROM source_tables
              WHERE source_table_name = '{source_table_name}'"""

  cursor.execute(query)
  tables = cursor.fetchall()
  source_type = ""
  for table_row in tables: #sources usually only has one row
    source_type = str(table_row[0])
  model_path = model_path
  with open(os.path.join(".","templates","coalesce", "landing.yaml"),"r") as f:
      command = f.read()
  f.close()
  if source_type != 'snowflake_external_table_surrogate':
    prefix = config.get("landing", "PREFIX")
  else:
    prefix = config.get("landing", "PREFIX_EXT")

 
  landing_location = str(config.get("landing", "LOCATION")).upper()
  landing_table_name = str(prefix + '_' + source_table_name).upper()
  landing_table_id = landing_location + '__' + landing_table_name
  columns = get_source_attributes(cursor, source_table_name, landing_table_id, source_type)

  command = command.replace("@@landing_table_uuid", create_uuid(landing_table_id))
  command = command.replace("@@landing_table_name", landing_table_name)
  command = command.replace("@@landing_location", landing_location)
  command = command.replace("@@columns",columns)

  filename = os.path.join(model_path, f"{landing_location}-{landing_table_name}.yml")

  path =model_path

  # Check whether the specified path exists or not
  isExist = os.path.exists(path)
  if not isExist:   
  # Create a new directory because it does not exist 
      os.makedirs(path)

  with open(filename, 'w') as f:
    f.write(command.expandtabs(2))

  print(f"Created model \'{filename}\'")