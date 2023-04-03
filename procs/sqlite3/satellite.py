from numpy import object_
import os
import procs.sqlite3.helper as helper
TEST_SAT = """
version: 2
models:
  - name: {table_name}
    tags:
      - {object_name}
    description: Satellit for {object_name}
    columns:
      - name: {hub_hash_key}
        tests:
          - not_null
          - relationships:
              to: ref('{object_name}')
              field: {hub_hash_key}
    tests:
       - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
             - {hub_hash_key}
             - ldts
"""    
def gen_payload(payload_list,effective_date_type='', effective_date_attribute='', hashkey_column=''):
    payload_string = ''
    
    for column in payload_list:
        payload_string = payload_string + f'\t- {column.lower()}\n'
    
    if effective_date_type=='Type 1':
        payload_string = payload_string  + f'\t- {effective_date_attribute.lower()}\n'
        payload_string = payload_string  + f'\t- {hashkey_column.lower().replace("hk_", "hke_")}\n'
    return payload_string

def gen_ma_key(satellite_ma_list):
    ma_key_string = ''
    for column in satellite_ma_list:
        ma_key_string = ma_key_string + f'\t- {column.lower()}\n'
    return ma_key_string

def gen_ma_key_unique(satellite_ma_list):
    ma_key_unique_string = ''
    ma_key_unique_string = ', '.join(f"'{column.lower()}'" for column in satellite_ma_list)
    return ma_key_unique_string

def generate_satellite_list(cursor, source):

    source_name, source_object = helper.source_split(source)

    query = f"""
    SELECT DISTINCT 
          Satellite_Identifier
        , Target_Satellite_Table_Physical_Name
        , Hub_Primary_Key_Physical_Name
        , GROUP_CONCAT(Target_Column_Physical_Name)
        , Source_Table_Physical_Name,Load_Date_Column
        , effective_date_type
        , effective_date_attribute 
        , is_ref_object  
        , business_object_name             
    from 
    (
        SELECT DISTINCT 
              hs.Satellite_Identifier
            , hs.Target_Satellite_Table_Physical_Name
            , hs.Hub_Primary_Key_Physical_Name
            , hs.Target_Column_Physical_Name
            , src.Source_Table_Physical_Name
            , src.Load_Date_Column 
            , src.effective_date_type
            , src.effective_date_attribute 
            , hs.is_ref_object
            , hs.business_object_name
        FROM hub_satellites hs
        inner join source_data src 
            on src.Source_table_identifier = hs.Source_Table_Identifier
        where hs.ma_attribute is false -- it is no multiactive attribute
        and src.Source_System = '{source_name}'
        and src.Source_Object = '{source_object}'
        order by hs.Target_Column_Sort_Order asc
    )
    group by Satellite_Identifier,Target_Satellite_Table_Physical_Name,Hub_Primary_Key_Physical_Name,Source_Table_Physical_Name,Load_Date_Column, effective_date_type, effective_date_attribute, is_ref_object, business_object_name


    UNION

    SELECT DISTINCT 
        Satellite_Identifier
        ,Target_Satellite_Table_Physical_Name
        ,Link_primary_key_physical_name
        ,GROUP_CONCAT(Target_Column_Physical_Name)
        ,Source_Table_Physical_Name,Load_Date_Column
        , effective_date_type
        , effective_date_attribute 
        , false as is_ref_object
        , NULL as business_object_name
        FROM
        (
            SELECT DISTINCT 
                ls.Satellite_Identifier
                ,ls.Target_Satellite_Table_Physical_Name
                ,ls.Link_primary_key_physical_name
                ,ls.Target_Column_Physical_Name
                ,src.Source_Table_Physical_Name
                ,src.Load_Date_Column
                , src.effective_date_type
                , src.effective_date_attribute 
            from link_satellites ls
            inner join source_data src 
                on src.Source_table_identifier = ls.Source_Table_Identifier
            where 1=1
            and src.Source_System = '{source_name}'
            and src.Source_Object = '{source_object}'
            order by Target_Column_Sort_Order asc
        )
        group by Satellite_Identifier,Target_Satellite_Table_Physical_Name,Link_primary_key_physical_name,Source_Table_Physical_Name,Load_Date_Column, effective_date_type, effective_date_attribute 

"""

    cursor.execute(query)
    results = cursor.fetchall()

    return results

def generate_satellite_ma_list(cursor, source):

    source_name, source_object = helper.source_split(source)

    query = f"""SELECT DISTINCT 
                      Satellite_Identifier
                    , Target_Satellite_Table_Physical_Name
                    , Hub_Primary_Key_Physical_Name
                    , GROUP_CONCAT(Target_Column_Physical_Name)
                    , Source_Table_Physical_Name
                    , Load_Date_Column
                    , effective_date_type
                    , effective_date_attribute 
                from 
                (
                    SELECT DISTINCT 
                          hs.Satellite_Identifier
                        , hs.Target_Satellite_Table_Physical_Name
                        , hs.Hub_Primary_Key_Physical_Name
                        , hs.Target_Column_Physical_Name
                        , src.Source_Table_Physical_Name
                        , src.Load_Date_Column 
                        , hs.effective_date_type
                        , hs.effective_date_attribute 
                    FROM hub_satellites hs
                    inner join source_data src 
                        on src.Source_table_identifier = hs.Source_Table_Identifier
                    where hs.ma_attribute is TRUE -- it is a multiactive attribute
                    and src.Source_System = '{source_name}'
                    and src.Source_Object = '{source_object}'
                    order by Target_Column_Sort_Order asc)
                    group by Satellite_Identifier,Target_Satellite_Table_Physical_Name,Hub_Primary_Key_Physical_Name,Source_Table_Physical_Name,Load_Date_Column
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results
      

def generate_satellite(cursor,source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming):
    
    satellite_list = generate_satellite_list(cursor=cursor, source=source)
    
    satellite_ma_list = generate_satellite_ma_list(cursor=cursor, source=source)
    
    source_name, source_object = helper.source_split(source)
    model_path_v0 = model_path.replace('@@entitytype','dwh_04_rv').replace('@@SourceSystem',source_name)
    model_path_v1 = model_path.replace('@@entitytype','dwh_04_rv').replace('@@SourceSystem',source_name)

    for satellite in satellite_list:
        satellite_name = satellite[1]
        hashkey_column = satellite[2]
        hashdiff_column = hashdiff_naming.replace('@@SatName',satellite_name)
        payload_list = satellite[3].split(',')
        source_model = satellite[4].lower().replace('load', 'stg')
        loaddate = satellite[5]
        effective_date_type = satellite[6]
        effective_date_attribute = satellite[7]
        is_ref_object = str(satellite[8]).strip()=='1'
        business_object_name = satellite[9]

        payload = gen_payload(payload_list, effective_date_type, effective_date_attribute, hashkey_column)

        if not satellite_ma_list: # This is no ma Satellite
            if not is_ref_object:
                template_path = os.path.join(".","templates","sat_v0.txt")
            else:
                template_path = os.path.join(".","templates","ref_sat_v0.txt")

            #Satellite_v0
            with open(template_path,"r") as f:
                command_tmp = f.read()
            f.close()
            command_v0 = command_tmp.replace('@@SourceModel', source_model).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@Payload', payload).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
            command_v0 = command_v0.replace('@@parent_ref_keys', hashkey_column)    
    
            satellite_model_name_splitted_list = satellite_name.split('_')

            #satellite_model_name_splitted_list[-2] += '0'

            satellite_model_name_v0 = '_'.join(satellite_model_name_splitted_list)
            if business_object_name is None or not is_ref_object:
                business_object = satellite_name.split('_')[0]        
            else:
                business_object = business_object_name
            if not is_ref_object:
                filename = os.path.join(model_path_v0 , business_object, f"{satellite_model_name_v0}.sql")
                path = os.path.join(model_path_v0, business_object)
            else:
                filename = os.path.join(model_path_v0 , "reference", business_object, f"{satellite_model_name_v0}.sql")
                path = os.path.join(model_path_v0, "reference", business_object)
                

            # Check whether the specified path exists or not
            isExist = os.path.exists(path)

            if not isExist:   
            # Create a new directory because it does not exist 
                os.makedirs(path)

            with open(filename, 'w') as f:
                f.write(command_v0.expandtabs(2))
                if not is_ref_object:
                    print(f"Created Satellite Model {satellite_model_name_v0}")
                else:
                    print(f"Created Reference Satellite Model {satellite_model_name_v0}")
        # Create Test 
            if not is_ref_object:
                filename = os.path.join(model_path_v0 , business_object, f"test_{satellite_model_name_v0}.yaml")
                business_object = business_object + '_h'

            else:
                filename = os.path.join(model_path_v0 , "reference", business_object, f"test_{satellite_model_name_v0}.yaml")
                business_object = business_object + '_r'

            yaml = TEST_SAT.format(table_name=satellite_model_name_v0, object_name=business_object, hub_hash_key=hashkey_column)

            with open(filename, 'w') as f:
                f.write(yaml.expandtabs(2)) 
                print(f"Created Sat Test {satellite_model_name_v0}")                     
            #Satellite_v1
            # with open(os.path.join(".","templates","sat_v1.txt"),"r") as f:
            #     command_tmp = f.read()
            # f.close()
            # command_v1 = command_tmp.replace('@@SatName', satellite_model_name_v0).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
                
            # filename_v1 = os.path.join(model_path_v1 , business_object,  f"{satellite_name}.sql")
                    
            # path_v1 = os.path.join(model_path_v1, business_object)

            # # Check whether the specified path exists or not
            # isExist_v1 = os.path.exists(path_v1)

            # if not isExist_v1:   
            # # Create a 
            # new directory because it does not exist 
            #     os.makedirs(path_v1)

            # with open(filename_v1, 'w') as f:
            #     f.write(command_v1.expandtabs(2))
            #     print(f"Created Satellite Model {satellite_name}")
                
        else: # This is a ma Satellite
            print("This is ma Satellite")
            for satellite in satellite_ma_list:
                ma_key_list = satellite[3].split(',')
            ma_key = gen_ma_key(ma_key_list)
            ma_key_unique = gen_ma_key_unique(ma_key_list)
            
            #Satellite_v0
            with open(os.path.join(".","templates","sat_ma_v0.txt"),"r") as f:
                command_tmp = f.read()
            f.close()
            command_v0 = command_tmp.replace('@@SourceModel', source_model).replace('@@Hashkey', hashkey_column).replace('@@MaKeyUnique', ma_key_unique).replace('@@MaKey', ma_key).replace('@@Hashdiff', hashdiff_column).replace('@@Payload', payload).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
                
            satellite_model_name_splitted_list = satellite_name.split('_')
            satellite_model_name_splitted_list[-1] = 'ms'        
            # print(satellite_model_name_splitted_list)
            satellite_model_name_v1 = '_'.join(satellite_model_name_splitted_list)
            #satellite_model_name_splitted_list[-2] += '0'
            # print(satellite_model_name_splitted_list)
            #satellite_model_name_splitted_list.insert(-1, 'm')
            satellite_model_name_v0 = '_'.join(satellite_model_name_splitted_list)
            
            business_object = satellite_name.split('_')[0]        

            filename = os.path.join(model_path_v0 , business_object, f"{satellite_model_name_v0}.sql")
                    
            path = os.path.join(model_path_v0, business_object)

            # Check whether the specified path exists or not
            isExist = os.path.exists(path)

            if not isExist:   
            # Create a new directory because it does not exist 
                os.makedirs(path)

            with open(filename, 'w') as f:
                f.write(command_v0.expandtabs(2))
                print(f"Created Satellite Model {satellite_model_name_v0}")

            # #Satellite_v1
            # with open(os.path.join(".","templates","sat_ma_v1.txt"),"r") as f:
            #     command_tmp = f.read()
            # f.close()
            # command_v1 = command_tmp.replace('@@SatName', satellite_model_name_v0).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@MaKey', ma_key).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
                
            # filename_v1 = os.path.join(model_path_v1 , business_object,  f"{satellite_model_name_v1}.sql")
                    
            # path_v1 = os.path.join(model_path_v1, business_object)

            # # Check whether the specified path exists or not
            # isExist_v1 = os.path.exists(path_v1)

            # if not isExist_v1:   
            # # Create a new directory because it does not exist 
            #     os.makedirs(path_v1)

            # with open(filename_v1, 'w') as f:
            #     f.write(command_v1.expandtabs(2))
            #     print(f"Created multi-active Satellite Model {satellite_model_name_v1}")            