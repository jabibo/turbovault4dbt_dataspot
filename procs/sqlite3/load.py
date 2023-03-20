import codecs
from datetime import datetime
import os

def add_payload(payload_string, key, value):
    if key != "" and value != "":
      value = str(value).strip(' ')
      key = str(key).strip(' ')
      payload_string = payload_string + f'{key}: {value}\n'
    
    return payload_string

def generate_load_table_attributes(cursor, source_short ,source_table_name, selection):
  #print(source_short + '-' + source_table_name + '-' + selection)
  query = f"""SELECT 
                     source_short
                   , source_table_name
                   , attribute_name
                   , DataType
                   , format
                   , source_column_number
                   , type_check
                   , decimal_separator
                   , value
              FROM load_table_attributes
              WHERE source_short = '{source_short}' 
              and source_table_name = '{source_table_name}'
              and selection = '{selection}'
              order by source_short, source_table_name, source_column_number
"""           
  cursor.execute(query)
  source_attributes = cursor.fetchall()
  attributes = selection + ":\n"
  tab = "  "
  
  for row in source_attributes:
    attributes = attributes  + tab*2 + row[2] + ":\n"
    attributes = add_payload(attributes + tab*3 , 'data_type', row[3])
    if row[4] != "":
      attributes = add_payload(attributes + tab*3 , 'format', row[4])
    if row[5] >= '0':
      attributes = add_payload(attributes + tab*3 , 'source_column_number', row[5])
    if row[7] != "":
      attributes = add_payload(attributes + tab*3 , 'decimal_separator', row[7])
    type_check = 'True' if row[6].lower()=='1' else 'False'
    if type_check == 'True':
      attributes = add_payload(attributes + tab*3 , 'type_check', type_check)
    if row[8] != "":
      attributes = add_payload(attributes + tab*3 , 'value', row[8])


  return attributes
   

def generate_load(cursor, source, model_path):
  source_long, source_object = source.split("_")
  #print("generate: " + source)

  query = f"""SELECT 
                  source_short
                , source_table_name
                , source_database
                , dbt_source_name
                , is_hwm
                , dub_check
                , key_check
                , source_type
                , target_table_name
                , materialization
                , pre_hook
                , post_hook
                , source_table_name_long
              FROM load_tables
              WHERE source_name = '{source_long}' 
              and source_table_name = '{source_object}'"""
  
  cursor.execute(query)
  sources = cursor.fetchall()
  columns = ''
  additional_columns = ''
  default_columns = ''
  
  for row in sources: 
    source_short= row[0]
    columns = generate_load_table_attributes( 
                      cursor=cursor
                    , source_short =source_short
                    , source_table_name = row[1]
                    , selection='columns'
                   )
    additional_columns = generate_load_table_attributes( 
                      cursor=cursor
                    , source_short =source_short
                    , source_table_name = row[1]
                    , selection='additional_columns'
                   )
    default_columns = generate_load_table_attributes( 
                      cursor=cursor
                    , source_short =source_short
                    , source_table_name = row[1]
                    , selection='default_columns'
                   )
    #print("default_columns:" + default_columns)
    generate_load_sql( 
                    source_short  
                  , model_path
                  , source_table_name=row[1]
                  , source_database=row[2]
                  , dbt_source_name=row[3]
                  , is_hwm=row[4]
                  , dub_check=row[5]
                  , key_check=row[6]
                  , source_type=row[7]
                  , columns=columns
                  , default_columns=default_columns
                  , additional_columns=additional_columns 
                  , target_table_name=row[8]
                  , materialization=row[9]
                  , pre_hook=row[10]
                  , post_hook=row[11]
                  , source_table_name_long=row[12]

)


def generate_load_sql(    
                     source_name
                    , model_path
                    , source_table_name
                    , source_database
                    , dbt_source_name
                    , is_hwm
                    , dub_check
                    , key_check
                    , source_type
                    , columns
                    , default_columns
                    , additional_columns
                    , target_table_name
                    , materialization
                    , pre_hook
                    , post_hook
                    , source_table_name_long):
  model_path = model_path.replace("@@entitytype", "dwh_02_load").replace("@@SourceSystem", source_name)

  with open(os.path.join(".","templates","load.txt"),"r") as f:
      command_tmp = f.read()
  f.close()
  command = command_tmp.replace("@@source_table", str(source_table_name_long).upper())
  command = command.replace("@@source_database",source_database)
  command = command.replace("@@dbt_source_name",dbt_source_name)
  command = command.replace("@@is_hwm",is_hwm)
  command = command.replace("@@dub_check",dub_check)
  command = command.replace("@@key_check",key_check)
  command = command.replace("@@source_type",source_type)
  command = command.replace("@@columns", columns)
  command = command.replace("@@default_columns", default_columns)
  command = command.replace("@@additional_columns", additional_columns)
  command = command.replace("@@materialization", materialization)
  command = command.replace("@@pre_hook", pre_hook.replace('"', '\''))
  command = command.replace("@@post_hook", post_hook.replace('"', '\''))

  filename = os.path.join(model_path, f"{target_table_name.lower()}.sql")

  path =model_path

  # Check whether the specified path exists or not
  isExist = os.path.exists(path)
  if not isExist:   
  # Create a new directory because it does not exist 
      os.makedirs(path)

  with open(filename, 'w') as f:
    f.write(command.expandtabs(2))

  print(f"Created model {filename.lower()}")