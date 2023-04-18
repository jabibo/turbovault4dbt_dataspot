import os
import procs.sqlite3.helper as helper

TEST_HUB = """
version: 2
models:
  - name: {table_name}
    tags:
      - {object_name}
    description: "Hub of {object_name}"
    columns:
      - name: {hub_hash_key}
        description: "Hashkey of {object_name}"
        tests:
          - not_null
    tests:
       - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
             - ldts          
             {key_list}

"""    


def generate_hub_list(cursor, source):

    source_name, source_object = helper.source_split(source)

    query = f"""SELECT 
                      Hub_Identifier
                    , Target_Hub_table_physical_name
                    , GROUP_CONCAT(Business_Key_Physical_Name)  
                    , is_ref_object
                    , business_object_name
                from 
                (
                    SELECT distinct 
                          h.hub_identifier
                        , h.Target_Hub_table_physical_name
                        , h.Business_Key_Physical_Name
                        , h.is_ref_object
                        , h.business_object_name
                    FROM hub_entities h
                    inner join source_data src 
                        on src.Source_table_identifier = h.Source_Table_Identifier
                    where 1=1
                    and is_nh_link=0                    
                    and src.Source_System = '{source_name}'
                    and src.Source_Object = '{source_object}'
                    ORDER BY h.Target_Column_Sort_Order
                )
                group by Hub_Identifier,Target_Hub_table_physical_name, is_ref_object,business_object_name
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results


def generate_source_models(cursor, hub_id):

    command = ""

    query = f"""SELECT 
                      Source_Table_Physical_Name
                    , GROUP_CONCAT(business_key_physical_name)
                    , Static_Part_of_Record_Source_Column
                FROM 
                (
                    SELECT distinct 
                              src.Source_Table_Physical_Name
                            , h.business_key_physical_name
                            , src.Static_Part_of_Record_Source_Column 
                    FROM hub_entities h
                    inner join source_data src 
                        on h.Source_Table_Identifier = src.Source_table_identifier
                where 1=1
                and Hub_Identifier = '{hub_id}'
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
        
        command += f"\n\t{source_table_name}:\n\t\tbk_columns: {bk_col_output}"

        rsrc_static = source_table_row[2]

        if rsrc_static != '':
            command += f"\n\t\trsrc_static: '{rsrc_static}'"

    return command


def generate_hashkey(cursor, hub_id):

    query = f"""SELECT DISTINCT Target_Primary_Key_Physical_Name 
                FROM hub_entities
                WHERE hub_identifier = '{hub_id}'"""

    cursor.execute(query)
    results = cursor.fetchall()

    for hashkey_row in results: #Usually a hub only has one hashkey column, so results should only return one row
        hashkey_name = hashkey_row[0] 

    return hashkey_name
            

def generate_hub(cursor,source, generated_timestamp,rdv_default_schema,model_path):

    hub_list = generate_hub_list(cursor=cursor, source=source)

    source_name, source_object = helper.source_split(source)
    model_path = model_path.replace('@@entitytype','dwh_04_rv').replace('@@SourceSystem',source_name)
    for hub in hub_list:

        hub_name = hub[1]
        hub_id = hub[0]
        is_ref_object = str(hub[3]).strip()=='1.0'
        business_object_name = hub[4]
        bk_list = hub[2].split(',')
        bk_string = ""
        for bk in bk_list:
            bk_string += f"\n\t- '{bk}'"

        # print("hub: " + hub_name + ":" + str(str(hub[3])) + ":" + str(is_ref_object))

        source_models = generate_source_models(cursor, hub_id).replace('load', 'stg')

        hashkey = generate_hashkey(cursor, hub_id)
        if is_ref_object:
            template_name = "ref_hub.txt"
        else:
            template_name = "hub.txt"
        with open(os.path.join(".","templates",template_name),"r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@Schema', rdv_default_schema).replace('@@SourceModels', source_models).replace('@@Hashkey', hashkey).replace('@@BusinessKeys', bk_string)
        command = command.replace('@@ref_keys',  bk_string)
        if business_object_name is None or not is_ref_object:   
            business_object = hub_name.split('_')[0]
        else:
            business_object = business_object_name
        #print("is_ref_object:" +business_object + " " + str(is_ref_object) )
        if is_ref_object:
            path = os.path.join(model_path,"reference", business_object)
            filename = os.path.join(model_path ,"reference", business_object, f"{hub_name}.sql")
        else:   
            path = os.path.join(model_path, business_object)
            filename = os.path.join(model_path , business_object, f"{hub_name}.sql")

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            print(f"Created Hub Model {hub_name}")  
 
        ### test

        if is_ref_object:
            hashkey = bk_list[0]
            key_list = bk_string.strip()
        else:   
            hashkey = hashkey
            key_list = '- ' + hashkey

        filename = os.path.join(path, f"test_{hub_name}.yaml")
        yaml = TEST_HUB.format(table_name=hub_name, object_name=business_object, hub_hash_key=hashkey, key_list=key_list)

        with open(filename, 'w') as f:
            f.write(yaml.expandtabs(2)) 
            print(f"Created Hub Test {hub_name}") 