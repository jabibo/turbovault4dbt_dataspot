import codecs
from datetime import datetime
import os


def generate_landing_zone(cursor, source,model_path):
  source_name, source_object = source.split("_")
  query = f"""SELECT 
                  source_short
                , dbt_source_name
                , external_table_pattern
                , external_table_fileformat
                , external_table_schema
                , external_table_location
                , source_database
                , external_table_description
                , external_table_name
                , source_type
              FROM landing_zone
              WHERE source_long = '{source_name}' 
              and source_table_name = '{source_object}'"""
  
  cursor.execute(query)
  sources = cursor.fetchall()
  for row in sources: #sources usually only has one row
    if row[9] == 'snowflake_external_table':
      generate_snowflake_external_table( source_name  
                                        , model_path
                                        , row[2]
                                        , row[3]
                                        , row[4]
                                        , row[5]
                                        , row[6]
                                        , row[7]
                                        , row[8]
                                        , )


def generate_snowflake_external_table(  source_name
                                      , model_path
                                      , external_table_pattern
                                      , external_table_fileformat
                                      , external_table_schema
                                      , external_table_location
                                      , source_database
                                      , external_table_description
                                      , external_table_name
                                      , ):
  model_path = model_path.replace("@@entitytype", "dwh_01_ext").replace("@@SourceSystem", source_name)

  with open(os.path.join(".","templates","landing_zone.txt"),"r") as f:
      command_tmp = f.read()
  f.close()
  command = command_tmp.replace("@@external_table_pattern",external_table_pattern)
  command = command.replace("@@external_table_fileformat",external_table_fileformat)
  command = command.replace("@@external_table_schema",external_table_schema)
  command = command.replace("@@external_table_location",external_table_location)
  command = command.replace("@@source_database",source_database)
  if external_table_description is None:
    external_table_description = ""
  command = command.replace('@@external_table_description',external_table_description)
  command = command.replace("@@external_table_name", external_table_name.upper() )     
  target_table_name = external_table_name.lower() 

  filename = os.path.join(model_path, f"{target_table_name.lower()}.yml")

  path =model_path

  # Check whether the specified path exists or not
  isExist = os.path.exists(path)
  if not isExist:   
  # Create a new directory because it does not exist 
      os.makedirs(path)

  with open(filename, 'w') as f:
    f.write(command.expandtabs(2))

  print(f"Created model \'{target_table_name.lower()}.sql\'")