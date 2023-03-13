from numpy import object_
import os


def generate_satellite_list(cursor, source):

    source_name, source_object = source.split("_")

    query = f"""SELECT 
                replace(business_key_physical_name, '_bk', '')||'_' || SUBSTRING(source_table_identifier, 1,  instr(source_table_identifier, '_')-1)||'_sts' as satellite_identifier
                , replace(business_key_physical_name, '_bk', '')||'_' || SUBSTRING(source_table_identifier, 1,  instr(source_table_identifier, '_')-1)||'_sts' as Target_Satellite_Table_Physical_Name
                , target_primary_key_physical_name hub_primary_key_physical_name
                , '' payload
                , source_table_physical_name 
                , 'ldts' as load_date_column
                , NULL as driving_key                 
                , NULL list_fks
                , NULL target_link_table_physical_name
                from hub_entities he 
                where source_table_identifier='{source}'
                and has_statustracking
                union all
                SELECT 
                satellite_identifier
                , Target_Satellite_Table_Physical_Name
                , hub_primary_key_physical_name
                , payload
                , source_table_physical_name 
                , load_date_column
                , driving_key 
                , list_fks
                , target_link_table_physical_name
                from 
                (
	                SELECT distinct
	                link_identifier||'_'||Source_system||'_sts' as satellite_identifier
	                , link_identifier||'_'||Source_system||'_sts'  as Target_Satellite_Table_Physical_Name
	                , target_primary_key_physical_name hub_primary_key_physical_name
	                ,  '' payload
	                , source_data.source_table_physical_name 
	                , 'ldts' as load_date_column
	                , 'hk_'||driving_key||'_h' as driving_key
	                , group_concat(target_column_physical_name, ',') list_fks
	                , target_link_table_physical_name
	                from link_entities
	                inner join source_data
	                on link_entities.Source_Table_Identifier = source_data.source_table_identifier 
	                where link_entities.source_table_identifier='{source}'
	                and link_entities.has_statustracking
	             ) a where satellite_identifier is not null
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results

def generate_st_satellite(cursor,source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming):
    
    satellite_list = generate_satellite_list(cursor=cursor, source=source)
    
    source_name, source_object = source.split("_")
    model_path_v0 = model_path.replace('@@entitytype','dwh_04_rv').replace('@@SourceSystem',source_name)
    model_path_v1 = model_path.replace('@@entitytype','dwh_04_rv').replace('@@SourceSystem',source_name)

    for satellite in satellite_list:
        satellite_name = satellite[1]
        hashkey_column = satellite[2]
        hashdiff_column = hashdiff_naming.replace('@@SatName',satellite_name)
        payload_list = satellite[3].split(',')
        source_model = satellite[4].lower().replace('load', 'stg')
        loaddate = satellite[5]
        driving_key = satellite[6]
        esat_link_name = satellite[8]
        list_secondary_fks =[]
        fks = satellite[7]
        if fks is not None:
            list_fks = fks.split(",")
            list_secondary_fks = [fks for fks in list_fks if fks != driving_key]

        secondary_fks=','.join(list_secondary_fks)

               
        #st_sat_v0
        with open(os.path.join(".","templates","st_sat_v0.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command_v0 = command_tmp.replace('@@SourceModel', source_model).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
            

        satellite_model_name_splitted_list = satellite_name.split('_')

        #satellite_model_name_splitted_list[-2] += '0'

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
            print(f"Created Status Satellite Model {satellite_model_name_v0}")

        # Define a effectivity_sat, if a driving_key is available
        if driving_key != None:
            #e_sat_v1
            with open(os.path.join(".","templates","e_sat_v1.txt"),"r") as f:
                command_tmp = f.read()
            f.close()
            command_v0 = command_tmp.replace('@@StsSats', satellite_name).replace('@@LinkHashkey', hashkey_column).replace('@@DrivingKey', driving_key).replace('@@SecondaryFks', secondary_fks).replace('@@LinkName', esat_link_name).replace('@@LoadDate', loaddate)
                

            satellite_model_name_splitted_list = satellite_name.split('_')

            satellite_model_name_splitted_list.pop()
            satellite_model_name_splitted_list.append('es')

            satellite_model_name_esat = '_'.join(satellite_model_name_splitted_list)

            business_object = satellite_name.split('_')[0]        

            filename = os.path.join(model_path_v0 , business_object, f"{satellite_model_name_esat}.sql")
                    
            path = os.path.join(model_path_v0, business_object)

            # Check whether the specified path exists or not
            isExist = os.path.exists(path)

            if not isExist:   
            # Create a new directory because it does not exist 
                os.makedirs(path)

            with open(filename, 'w') as f:
                f.write(command_v0.expandtabs(2))
                print(f"Created Effectivity Satellite Model {satellite_model_name_esat}")


        # #st_sat_v1
        # with open(os.path.join(".","templates","st_sat_v1.txt"),"r") as f:
        #     command_tmp = f.read()
        # f.close()
        # command_v1 = command_tmp.replace('@@SatName', satellite_model_name_v0).replace('@@Hashkey', hashkey_column).replace('@@Hashdiff', hashdiff_column).replace('@@LoadDate', loaddate).replace('@@Schema', rdv_default_schema)
            
        # filename_v1 = os.path.join(model_path_v1 , business_object,  f"{satellite_name}.sql")
                
        # path_v1 = os.path.join(model_path_v1, business_object)

        # # Check whether the specified path exists or not
        # isExist_v1 = os.path.exists(path_v1)

        # if not isExist_v1:   
        # # Create a new directory because it does not exist 
        #     os.makedirs(path_v1)

        # with open(filename_v1, 'w') as f:
        #     f.write(command_v1.expandtabs(2))
        #     print(f"Created Satellite Model {satellite_name}")
            
