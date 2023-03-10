import codecs
from datetime import datetime
import os


def generate_landing_zone(cursor, source,model_path):
  source_name, source_object = source.split("_")
  query = f"""SELECT 
                  source_name
                , dbtSourceName
                , ExternalTablePattern
                , ExternalTableFileFormat
                , ExternalTableSchema
                , ExternalTableLocation
                , SourceDatabase
                , ExternalTableDescription
                , ExternalTableName
                , source_type
              FROM landing_zone
              WHERE source_name = '{source_name}' 
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
                                      , ExternalTablePattern
                                      , ExternalTableFileFormat
                                      , ExternalTableSchema
                                      , ExternalTableLocation
                                      , SourceDatabase
                                      , ExternalTableDescription
                                      , ExternalTableName
                                      , ):
  model_path = model_path.replace("@@entitytype", "dwh_01_ext").replace("@@SourceSystem", source_name)

  with open(os.path.join(".","templates","snowflake_external_table.txt"),"r") as f:
      command_tmp = f.read()
  f.close()
  command = command_tmp.replace("@@ExternalTablePattern",ExternalTablePattern)
  command = command.replace("@@ExternalTableFileFormat",ExternalTableFileFormat)
  command = command.replace("@@ExternalTableSchema",ExternalTableSchema)
  command = command.replace("@@ExternalTableLocation",ExternalTableLocation)
  command = command.replace("@@SourceDatabase",SourceDatabase)
  if ExternalTableDescription is None:
    ExternalTableDescription = ""
  command = command.replace('@@ExternalTableDescription',ExternalTableDescription)
  command = command.replace("@@ExternalTableName", ExternalTableName.upper() )     

  target_table_name = ExternalTableName.lower() 

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