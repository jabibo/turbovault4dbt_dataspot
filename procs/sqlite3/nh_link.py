import os

def generate_source_models(cursor, link_id):

    command = ""

    query = f"""SELECT Source_Table_Physical_Name,GROUP_CONCAT(link_primary_key_physical_name), '' Static_Part_of_Record_Source_Column
                FROM 
                (SELECT distinct src.Source_Table_Physical_Name,h.link_primary_key_physical_name ,src.Static_Part_of_Record_Source_Column FROM nh_link_entities h
                inner join source_data src on h.Source_Table_Identifier = src.Source_table_identifier
                where 1=1
                and link_Identifier = '{link_id}'
                ORDER BY h.Target_Column_Sort_Order)
                group by Source_Table_Physical_Name,Static_Part_of_Record_Source_Column
                """

    cursor.execute(query)
    results = cursor.fetchall()
    for source_table_row in results:
        source_table_name = source_table_row[0].lower()
        bk_columns = source_table_row[1].split(',')

        if len(bk_columns) > 1: 
            bk_col_output = ""
            for bk in bk_columns: 
                bk_col_output += f"\n\t\t\t- '{bk}'"
        else:
            bk_col_output = "'" + bk_columns[0] + "'"
        
        command += f"{source_table_name}"#:\n\t\tbk_columns: {bk_col_output}"

        rsrc_static = source_table_row[2]

        if rsrc_static != '':
            command += f"\n\t\trsrc_static: '{rsrc_static}'"

    return command

def generate_nh_link_list(cursor, source):

    query = f"""select 
                Link_Identifier,Target_link_table_physical_name,GROUP_CONCAT(Target_column_physical_name)
                from 
                (
                  SELECT distinct Link_Identifier,Target_link_table_physical_name, Target_column_physical_name from nh_link_entities
                  where source_table_identifier ='{source}' and title='BK'                
                ) 
                group by Link_Identifier,Target_link_table_physical_name
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results

def generate_link_hashkey(cursor, link_id):

    query = f"""SELECT DISTINCT link_primary_key_physical_name  
                FROM nh_link_entities
                WHERE link_identifier = '{link_id}'"""

    cursor.execute(query)
    results = cursor.fetchall()

    for link_hashkey_row in results: #Usually a link only has one hashkey column, so results should only return one row
        link_hashkey_name = link_hashkey_row[0] 

    return link_hashkey_name

def generate_payload_list(cursor, source):
  
    query = f"""SELECT 
                Link_Identifier,Target_link_table_physical_name,GROUP_CONCAT(Target_column_physical_name)
                from nh_link_entities
                where source_table_identifier ='{source}' and title<>'BK'
                group by Link_Identifier,Target_link_table_physical_name
                """

    cursor.execute(query)
    results = cursor.fetchall()
    #payload_list = results[2].split(',')
    return results  
  
def generate_nh_link(cursor, source, generated_timestamp, rdv_default_schema, model_path):

  nh_link_payload_list = generate_payload_list(cursor=cursor, source=source)
  payload_list = []
  for payload in nh_link_payload_list:
    payload_list = payload[2].split(',')
  payload = ''
    
  for column in payload_list:
        payload = payload + f'\t- {column.lower()}\n'
    
    
  link_list = generate_nh_link_list(cursor=cursor, source=source)
  for link in link_list:
    
    link_name = link[1]
    link_id = link[0]
    fk_list = link[2].split(',')

    fk_string = ""
    for fk in fk_list:
      fk_string += f"\n\t- '{fk}'"

    source_models = generate_source_models(cursor, link_id).replace('load', 'stg')
    link_hashkey = generate_link_hashkey(cursor, link_id)

    source_name, source_object = source.split("_")
    model_path = model_path.replace('@@entitytype','dwh_04_rv').replace('@@SourceSystem',source_name)



    with open(os.path.join(".","templates","nh_link.txt"),"r") as f:
        command_tmp = f.read()
    f.close()
    command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@SourceModels', source_models).replace('@@Payload', payload).replace('@@LinkHashkey', link_hashkey).replace('@@ForeignHashkeys', fk_string)
    
    business_object = link_name.split('_')[0]

    filename = os.path.join(model_path, business_object,  f"{link_name}.sql")

    path = os.path.join(model_path, business_object)

    # Check whether the specified path exists or not
    isExist = os.path.exists(path)

    if not isExist:   
    # Create a new directory because it does not exist 
      os.makedirs(path)

    with open(filename, 'w') as f:
      f.write(command.expandtabs(2))
      print(f"Created NH_Link Model {link_name}")
