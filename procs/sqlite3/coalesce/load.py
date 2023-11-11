import codecs
from datetime import datetime
import os
import procs.sqlite3.helper as helper
import hashlib
import uuid 
import yaml

snowflake_external_table_surrogate_attributes = [['', 'VALUE', 'VARIANT'], ['','FILENAMEDATE', 'STRING'], ['','METADATA$FILE_ROW_NUMBER', 'NUMBER(38,0)']]
def create_uuid(input_string:str):

  m = hashlib.md5()
  m.update(input_string.encode('utf-8'))
  return str(uuid.UUID(m.hexdigest()))

def get_transform_raw(column_type:str, attribute_type:str, attribute_name:str, attribute_transform:str, decimal_separator:str, source_column_number:str):
  if column_type in ['additional_columns','default_columns']:
    out_attribute = "TRIM(" + attribute_transform + "::STRING)"
  elif column_type in ['columns']:
    if attribute_type=="NUMBER":
            out_attribute =  "TRIM(value:c" + source_column_number + "::STRING) "
            out_attribute =  "REPLACE(" + out_attribute + ", ',', '" + decimal_separator + "')"
            out_attribute =  out_attribute + " as " + attribute_name +"_raw"
    else:
        out_attribute =  "TRIM(value:c" + source_column_number + "::STRING)"
  return out_attribute

def get_transform_typed( data_type:str, attribute_raw_definition:str, attribute_format:str):

  if attribute_format.strip(" ") == '':
    data_format = ''
  else:
    if data_type in ["TIMESTAMP","DATE"]:
      data_format =  ", '" + attribute_format + "'" 
    elif data_type in ["NUMBER"]:
      data_format =  ", " + attribute_format + ""

  if data_type in ["TIMESTAMP","DATE","NUMBER"]:
    out_attribute =  "TRY_TO_" + data_type + "(" + attribute_raw_definition  + "::STRING " + data_format + ")"
  else:
    out_attribute =  attribute_raw_definition 
  return out_attribute

def get_column_definition_typed(target_table_id, column_name, column_type, source_type, config, landing_table_name, landing_table_id, column_order, column_dataType, column_format, column_transform):

  column_description = '""'
  if column_type == 'columns' and source_type in ('snowflake_external_table_surrogate','snowflake_external_table' ):
    ext_json_column_template = str(config.get("landing", "EXT_JSON_COLUMN_NAME"))
    column_transform = ext_json_column_template.replace("@@landing_table_name", landing_table_name)
    column_transform = column_transform.replace("@@column_order", column_order)
    column_transform = get_transform_typed(data_type=column_dataType, attribute_raw_definition=column_transform, attribute_format=column_format) 
    landing_table_attribute_id = landing_table_id + '__' + str('VALUE')
    landing_table_attribute_uuid = create_uuid(landing_table_attribute_id)
    landing_table_uuid = create_uuid(landing_table_id)
    
  elif column_type in ('additional_columns','default_columns'):
    landing_table_attribute_uuid = '"0"'
    landing_table_uuid = '"0"'
    column_transform = column_transform.replace('@@source_table', landing_table_name).replace('"', '\\"') 
    column_transform = get_transform_typed(data_type=column_dataType, attribute_raw_definition=column_transform, attribute_format=column_format) 
  else:
    landing_table_attribute_id = landing_table_id + '__' + column_name.upper()
    landing_table_attribute_uuid = create_uuid(landing_table_attribute_id)
    landing_table_uuid = create_uuid(landing_table_id)
  
  column_id = target_table_id + '__' + column_name

  return column_id, column_name, column_description, landing_table_attribute_uuid, landing_table_uuid, column_transform


def get_column_definition_check(target_table_id, column_name, column_type, source_type, config, landing_table_name, landing_table_id, column_order, column_dataType, column_format, column_transform, type_check):
  column_name_check = column_name + '_CHECK_IS_OK'
  column_description = '""'
  column_id = '"0"'
  landing_table_attribute_uuid = '"0"'
  landing_table_uuid = '"0"'
  #print(column_name + '>' + column_transform + str(type_check))
  if source_type in ('snowflake_external_table_surrogate','snowflake_external_table'):
    if column_type == 'columns':
      ext_json_column_template = str(config.get("landing", "EXT_JSON_COLUMN_NAME"))
      column_transform = ext_json_column_template.replace("@@landing_table_name", landing_table_name)
      column_transform = column_transform.replace("@@column_order", column_order)
      column_transform = 'NOT(NOT '+ column_transform.replace("@@column_order", column_order) + " IS NULL AND " + get_transform_typed(data_type=column_dataType, attribute_raw_definition=column_transform, attribute_format=column_format) + 'IS NULL)'
      landing_table_attribute_id = landing_table_id + '__' + str('VALUE')
      landing_table_attribute_uuid = create_uuid(landing_table_attribute_id)
      landing_table_uuid = create_uuid(landing_table_id)
    elif column_type in ('additional_columns','default_columns'):
      landing_table_attribute_uuid = '"0"'
      landing_table_uuid = '"0"'
      column_transform = column_transform.replace('@@source_table', landing_table_name).replace('"', '\\"') + '::STRING'
      column_transform = 'NOT(NOT '+ column_transform.replace("@@column_order", column_order) + " IS NULL AND " + get_transform_typed(data_type=column_dataType, attribute_raw_definition=column_transform, attribute_format=column_format) + 'IS NULL)'
    
  column_id = target_table_id + '__' + column_name_check
  return column_id, column_name_check, column_description, landing_table_attribute_uuid, landing_table_uuid, column_transform

  command = command.replace("@@source_table_uuid",landing_table_uuid)    
def get_key_check(target_table_id, landing_table_id, landing_table_name, key_check_list:list):
  command_tmp = ""
  with open(os.path.join(".","templates","coalesce", "load-column.yaml"),"r") as f:
    command_tmp = f.read()
  f.close()
  column_name = 'IS_KEY_CHECK_OK'
  column_dataType = 'BOOLEAN'
  column_description = '""'
  column_id = target_table_id + '__' + column_name
  landing_table_attribute_uuid = '"0"'
  landing_table_uuid = create_uuid(landing_table_id)
  print(str(key_check_list))
  if key_check_list != []:
    column_list_str = ""
    for column in key_check_list:
      if column_list_str != "":column_list_str += ", "
      column_list_str = column_list_str + column
    column_transform = 'COUNT(*) OVER (PARTITION BY ' + column_list_str + ') = 1 '
  else: 
    column_transform = 'TRUE'

  print(column_transform)
  
  command = command_tmp.replace("@@column_uuid",create_uuid(column_id))
  command = command.replace("@@table_uuid",create_uuid(target_table_id))
  command = command.replace("@@column_name",column_name)
  command = command.replace("@@column_dataType",column_dataType)
  command = command.replace("@@column_description",column_description)    
  command = command.replace("@@source_column_uuid",landing_table_attribute_uuid)    
  command = command.replace("@@source_table_uuid",landing_table_uuid)    
  command = command.replace("@@transform", '"' +  column_transform + '"')

  return command

  return 1

def get_columns(cursor, source_table_name, landing_table_name, landing_table_id, target_table_id, source_type, config, key_check_list):
  command_tmp = ""
  with open(os.path.join(".","templates","coalesce", "load-column.yaml"),"r") as f:
    command_tmp = f.read()
  f.close()
  query = f"""SELECT 
                  source_attribute_name
                , data_type
                , format
                , selection
                , attribute_order
                , value
                , type_check
              FROM source_table_attributes
              WHERE source_table_name = '{source_table_name}'"""
  
  cursor.execute(query)
  columns = cursor.fetchall()
  return_value = ""
  key_check_typed_list = []
  for columns_row in columns:
    column_name = str(columns_row[0]).upper()
    column_dataType = str(columns_row[1])
    column_format = str(columns_row[2])
    column_type = str(columns_row[3])
    column_order = str(columns_row[4])
    column_transform = str(columns_row[5])
    type_check = str(columns_row[6])=='1'
    

    column_transform_in = column_transform
    column_description = '""'
    #typed
    column_id, column_name, column_description, landing_table_attribute_uuid, landing_table_uuid, column_transform = get_column_definition_typed(target_table_id, column_name, column_type, source_type, config, landing_table_name, landing_table_id, column_order, column_dataType, column_format, column_transform_in)
    command = command_tmp.replace("@@column_uuid",create_uuid(column_id))
    command = command.replace("@@table_uuid",create_uuid(target_table_id))
    command = command.replace("@@column_name",column_name)
    command = command.replace("@@column_dataType",column_dataType)
    command = command.replace("@@column_description",column_description)    
    command = command.replace("@@source_column_uuid",landing_table_attribute_uuid)    
    command = command.replace("@@source_table_uuid",landing_table_uuid)    
    command = command.replace("@@transform", '"' + column_transform + '"')
    return_value = return_value + command

    #print(column_name + "->" + str(list(map(str.upper,key_check_list))))
    if column_name in list(map(str.upper,key_check_list)):
      key_check_typed_list.append(column_transform)
      

    # check
    if type_check:
      column_id, column_name_check, column_description, landing_table_attribute_uuid, landing_table_uuid, column_transform = get_column_definition_check(target_table_id, column_name, column_type, source_type, config, landing_table_name, landing_table_id, column_order, column_dataType, column_format, column_transform_in, type_check)
      command = command_tmp.replace("@@column_uuid",create_uuid(column_id))
      command = command.replace("@@table_uuid",create_uuid(target_table_id))
      command = command.replace("@@column_name",column_name_check)
      command = command.replace("@@column_dataType",column_dataType)
      command = command.replace("@@column_description",column_description)    
      command = command.replace("@@source_column_uuid",landing_table_attribute_uuid)    
      command = command.replace("@@source_table_uuid",landing_table_uuid)    
      command = command.replace("@@transform", '"' + column_transform + '"')
      return_value = return_value + command

  command = get_key_check(target_table_id=target_table_id
                          , landing_table_id=landing_table_id
                          , landing_table_name=landing_table_name
                          , key_check_list= key_check_typed_list
                          )
  return_value = return_value + command
  return return_value

def generate_load(cursor, source_table_name, model_path, config):
  source_table_name = source_table_name.upper()
  query = f"""SELECT 
                  source_type
                , key_check
              FROM source_tables
              WHERE source_table_name = '{source_table_name}'"""
  cursor.execute(query)
  tables = cursor.fetchall()
  source_type = ""

  for source_table_row in tables: 
    source_type = str(source_table_row[0])
    key_check_list = yaml.safe_load(source_table_row[1]).get('key_check', None)


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
                        , config=config
                        , key_check_list=key_check_list
                        )

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