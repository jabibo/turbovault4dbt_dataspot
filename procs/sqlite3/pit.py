import os

def generate_pit_list(cursor):

    #there is no option to select which pit should be generated, it is not possible to define this based on a source
    query = f"""select 
                base_entities.identifier
                , base_entities.tracked_entity
                , base_entities.hashkey
                , base_entities.dimension_key
                , group_concat(satellites.satellite_identifier) as satellite_identifier
                from 
                (
                    select distinct
                    replace(hub_identifier, '_h', '') as identifier
                    , target_hub_table_physical_name as tracked_entity
                    , target_primary_key_physical_name as hashkey
                    , replace(target_primary_key_physical_name, '_h', '_d') dimension_key 
                    from hub_entities
                    union all
                    select distinct
                    link_identifier as identifier
                    , target_link_table_physical_name as tracked_entity
                    , target_primary_key_physical_name as hashkey
                    , replace(target_primary_key_physical_name, '_l', '_d') as dimension_key
                    from link_entities
                ) base_entities
                inner join 
                (
                    select 
                    target_primary_key_physical_name hashkey
                    , replace(hub_identifier, '_h','') ||'_'||source_short||'_sts' as satellite_identifier
                    from hub_entities
                    where has_statustracking = 1 
                    union 
                    select DISTINCT 
                    hub_primary_key_physical_name hashkey
                    , satellite_identifier
                    from hub_satellites
                    union 
                    select distinct
                    target_primary_key_physical_name hashkey
                    , link_identifier ||'_'||source_short||'_sts' as satellite_identifier
                    from link_entities
                    where has_statustracking = 1 
                    union 
                    select distinct
                    target_primary_key_physical_name hashkey
                    , link_identifier ||'_'||source_short||'_es' as satellite_identifier
                    from link_entities
                    where replace(link_identifier, driving_key, '')<>link_identifier -- effectivity-sat
                ) satellites
                on base_entities.hashkey=satellites.hashkey
                group by base_entities.identifier, base_entities.tracked_entity, base_entities.hashkey 
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results


def generate_pit(cursor, model_path):

    pit_list = generate_pit_list(cursor=cursor)

    model_path = model_path.replace('@@entitytype','dwh_05_sn')
    for pit in pit_list:
        pit_identifier = pit[0]
        pit_name = pit[0]
        tracked_entity = pit[1]
        hashkey= pit[2]
        dimension_key=pit[3]
        sat_list = pit[4].split(',')
        sat_string = ""
        for sat in sat_list:
            sat_string += f"\n\t- '{sat}'"

        with open(os.path.join(".","templates","pit.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@tracked_entity', tracked_entity).replace('@@Hashkey', hashkey).replace('@@sats', sat_string).replace('@@dimension_key', dimension_key).replace('@@pit_identifier', pit_identifier)
           
        business_object = pit_name.split('_')[0]
        
        pit_name += '_SNP'
        
        filename = os.path.join(model_path , business_object, f"{pit_name}.sql")
                
        path = os.path.join(model_path, business_object)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            print(f"Created Pit Model {pit_name}")  