import os

def generate_sns_list(cursor):

    #there is no option to select which sns should be generated, it is not possible to define this based on a source
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
                    where is_nh_link=0
                    and parent_child_hierarchy=''                    
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
                    ,satellite_identifier
                    from hub_satellites
                    where satellite_identifier not like '%_ms'                    
                    union 
                    select distinct
                    target_primary_key_physical_name hashkey
                    , case when replace(link_identifier, driving_key, '')<>link_identifier -- effectivity-sat
                        then link_identifier ||'_'||source_short||'_es'
                        else link_identifier ||'_'||source_short||'_sts' end as satellite_identifier
                    from link_entities
                    where has_statustracking = 1 or replace(link_identifier, driving_key, '')<>link_identifier
                ) satellites
                on base_entities.hashkey=satellites.hashkey
                group by base_entities.identifier, base_entities.tracked_entity, base_entities.hashkey 
                """

    cursor.execute(query)
    results = cursor.fetchall()

    return results


def generate_sns(cursor, model_path):

    sns_list = generate_sns_list(cursor=cursor)

    model_path = model_path.replace('@@entitytype','dwh_05_sn')
    for sns in sns_list:
        identifier = sns[0]
        pit_name = identifier+'_snp'
        sns_name = identifier+'_sns'
        business_object = identifier.split('_')[0]        
        base_entity = sns[1]
        hashkey= sns[2]
        sat_list = sns[4].split(',')
        sat_string = ""
        for sat in sat_list:
            sat_string += f"\n\t- '{sat}'"

        with open(os.path.join(".","templates","sns.txt"),"r") as f:
            command_tmp = f.read()
        f.close()
        command = command_tmp.replace('@@PitName', pit_name).replace('@@Hashkey', hashkey).replace('@@sats', sat_string).replace('@@BaseEntity', base_entity)
           
        filename = os.path.join(model_path , business_object, f"{sns_name}.sql")
                
        path = os.path.join(model_path, business_object)

        # Check whether the specified path exists or not
        isExist = os.path.exists(path)

        if not isExist:   
        # Create a new directory because it does not exist 
            os.makedirs(path)

        with open(filename, 'w') as f:
            f.write(command.expandtabs(2))
            print(f"Created SNS Model {sns_name}")  