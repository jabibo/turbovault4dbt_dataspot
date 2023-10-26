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

def get_source_attributes(cursor, source_table_id, source_type):
  command_tmp = ""
  with open(os.path.join(".","templates","coalesce", "source-column.yaml"),"r") as f:
    command_tmp = f.read()
  f.close()
  return_value = ""
  if source_type != 'snowflake_external_table_surrogate':
    query = f"""SELECT 
                  source_table_attribute_id
                , source_attribute_name
                , dataType
                , format
              FROM source_table_attributes
              WHERE source_table_id = '{source_table_id}'"""
  
    cursor.execute(query)
    source_columns = cursor.fetchall()
  else: 
    source_columns = snowflake_external_table_surrogate_attributes
    for source_columns_row in source_columns:  
       source_columns_row[0] = str(source_table_id + '__' + source_columns_row[1]).upper()

  for source_columns_row in source_columns:
    source_table_attribute_id = str(source_columns_row[0])
    column_name = str(source_columns_row[1])
    column_dataType = str(source_columns_row[2])
   
    command = command_tmp.replace("@@column_columnCounter",create_uuid(source_table_attribute_id))
    command = command.replace("@@column_stepCounter",create_uuid(source_table_id))
    command = command.replace("@@column_name",column_name)
    command = command.replace("@@column_dataType",column_dataType)
    
    return_value = return_value + command
  return return_value

def generate_source(cursor, source_table_name, model_path, config):
  source_table_name = source_table_name.upper()
  query = f"""SELECT 
                  source_table_name
                , source_table_id
                , source_type
              FROM source_tables
              WHERE source_table_name = '{source_table_name}'"""
  # print(query)
  cursor.execute(query)
  sources = cursor.fetchall()
  source_table_id = ""
  source_type = ""
  for source_table_row in sources: #sources usually only has one row
        source_table_id = str(source_table_row[1])
        source_type = str(source_table_row[2])

  print('generate_source: ' + source_table_id)
  model_path = model_path

  with open(os.path.join(".","templates","coalesce", "source.yaml"),"r") as f:
      command = f.read()
  f.close()
  if source_type != 'snowflake_external_table_surrogate':
    prefix = config.get("source", "PREFIX")
  else:
    prefix = config.get("source", "PREFIX_EXT")
  source_table_name = str(prefix + '_' + source_table_name).upper()
  columns = get_source_attributes(cursor, source_table_id, source_type)

  command = command.replace("@@source_table_uuid", create_uuid(source_table_id))
  command = command.replace("@@source_table_name", source_table_name)
  command = command.replace("@@columns",columns)

  filename = os.path.join(model_path, f"SOURCE-{source_table_name.upper()}.yml")

  path =model_path

  # Check whether the specified path exists or not
  isExist = os.path.exists(path)
  if not isExist:   
  # Create a new directory because it does not exist 
      os.makedirs(path)

  with open(filename, 'w') as f:
    f.write(command.expandtabs(2))

  print(f"Created model \'{filename}\'")