import codecs
from datetime import datetime
import os
import procs.sqlite3.helper as helper

def generate_status_tracking_satellite_list(cursor, source, object_identifier):

    query = f"""
    SELECT distinct
          replace(business_key_physical_name, '_bk', '')||'_' ||  source_short ||'_sts' as satellite_identifier
        , he.source_table_identifier
        , replace(business_key_physical_name, '_bk', '') as object_identifier
  from hub_entities he
  inner join source_data sd
    on he.source_table_identifier = sd.source_table_identifier
  where has_statustracking
  and he.source_table_identifier='{source}'
  and replace(business_key_physical_name, '_bk', '') = '{object_identifier}'
  union
  SELECT
      satellite_identifier
    , source_table_identifier
    , object_identifier
  from
  (
    SELECT distinct
            link_identifier as object_identifier
          , link_identifier||'_'||source_short||'_sts' as satellite_identifier
          , source_data.source_table_identifier
    from link_entities
    inner join source_data
        on link_entities.Source_Table_Identifier = source_data.source_table_identifier
    where link_entities.has_statustracking
    and link_entities.source_table_identifier='{source}'
  ) a
  where satellite_identifier is not null
  and object_identifier = '{object_identifier}'
"""

	                

    cursor.execute(query)
    results = cursor.fetchall()

    return results

def gen_target_objects(cursor,source, hashdiff_naming):
  
  command = ""

  source_name, source_object = helper.source_split(source)

  query = f"""
              SELECT  Target_Primary_Key_Physical_Name
                    , GROUP_CONCAT(Source_Column_Physical_Name)
                    , IS_SATELLITE 
                    , effective_date_type
                    , effective_date_attribute
                    , source_system_short
                    , business_object_name
              FROM 
              (
                SELECT 
                    h.Target_Primary_Key_Physical_Name
                  , h.Source_Column_Physical_Name
                  , FALSE as IS_SATELLITE
                  , src.effective_date_type
                  , src.effective_date_attribute
                  , src.source_system_short
                  , replace(hub_identifier, '_h', '') as business_object_name
                FROM hub_entities h
                inner join source_data src 
                  on h.Source_Table_Identifier = src.Source_table_identifier
                WHERE src.Source_System = '{source_name}' 
                and src.Source_Object = '{source_object}'
                and not coalesce(is_ref_object, false)
                ORDER BY h.Target_Column_Sort_Order
              ) 
              GROUP BY Target_Primary_Key_Physical_Name
              UNION ALL
              SELECT  Target_Primary_Key_Physical_Name
                    , GROUP_CONCAT(distinct Source_Column_Physical_Name)
                    , IS_SATELLITE 
                    , effective_date_type
                    , effective_date_attribute
                    , source_system_short
                    , business_object_name
              FROM
              (
                SELECT  l.Target_Primary_Key_Physical_Name
                      , l.Source_Column_Physical_Name
                      , FALSE as IS_SATELLITE
                      , src.effective_date_type
                      , src.effective_date_attribute
                      , src.source_system_short
                      , l.link_identifier as business_object_name
                FROM link_entities l
                inner join source_data src
                  on l.Source_Table_Identifier = src.Source_table_identifier
                WHERE src.Source_System = '{source_name}' 
                and src.Source_Object = '{source_object}'
                ORDER BY l.Target_Column_Sort_Order
              )
              group by Target_Primary_Key_Physical_Name
              UNION ALL
              SELECT  target_column_physical_name
                    , GROUP_CONCAT(Source_Column_Physical_Name)
                    , IS_SATELLITE 
                    , effective_date_type
                    , effective_date_attribute
                    , source_system_short
                    , business_object_name
              FROM
              (
                SELECT   l.link_identifier target_column_physical_name
                        , replace(replace(target_column_physical_name, 'hk_',''), '_h','_bk')  as Source_Column_Physical_Name
                        , FALSE as IS_SATELLITE
                        , src.effective_date_type
                        , src.effective_date_attribute
                        , src.source_system_short
                        , replace(l.link_identifier, '_nhl', '')   as business_object_name
                FROM nh_link_entities l
                inner join source_data src 
                  on l.Source_Table_Identifier = src.Source_table_identifier
                WHERE src.Source_System = '{source_name}' 
                  and src.Source_Object = '{source_object}'
                ORDER BY l.Target_Column_Sort_Order
              )
              group by target_column_physical_name              
              UNION ALL
              SELECT  Target_Satellite_Table_Physical_Name
                    , GROUP_CONCAT(Source_Column_Physical_Name)
                    , IS_SATELLITE 
                    , effective_date_type
                    , effective_date_attribute
                    , source_system_short
                    , business_object_name
              FROM 
              (
                SELECT '{hashdiff_naming.replace("@@SatName", "")}' || s.Target_Satellite_Table_Physical_Name as Target_Satellite_Table_Physical_Name
                      , s.Source_Column_Physical_Name
                      , TRUE as IS_SATELLITE
                      , src.effective_date_type
                      , src.effective_date_attribute
                      , src.source_system_short
                      , replace(replace(targetcolumn, '_bk', ''),'nk', '') as business_object_name
              FROM hub_satellites s
                inner join source_data src 
                  on s.Source_Table_Identifier = src.Source_table_identifier
                WHERE src.Source_System = '{source_name}' 
                  and src.Source_Object = '{source_object}'
                order by s.Target_Column_Sort_Order
              )
              group by Target_Satellite_Table_Physical_Name
              UNION ALL
              SELECT  Target_Satellite_Table_Physical_Name
                    , GROUP_CONCAT(Source_Column_Physical_Name)
                    , IS_SATELLITE 
                    , effective_date_type
                    , effective_date_attribute
                    , source_system_short
                    , business_object_name
              FROM
              (
                SELECT  '{hashdiff_naming.replace("@@SatName", "")}' || s.Target_Satellite_Table_Physical_Name as Target_Satellite_Table_Physical_Name
                      , s.Source_Column_Physical_Name
                      , TRUE as IS_SATELLITE
                      , src.effective_date_type
                      , src.effective_date_attribute
                      , src.source_system_short
                      , satellite_identifier as business_object_name
              FROM link_satellites s
              inner join source_data src 
                on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' 
                and src.Source_Object = '{source_object}'
              order by s.Target_Column_Sort_Order)
              group by Target_Satellite_Table_Physical_Name
              """
  cursor.execute(query)
  target_model_list = cursor.fetchall()
  target_model_def = ""
  satellites_dict = {}
  satellite_dict = {}
  for target_model in target_model_list:
    source_system_short = target_model[5]
    target_business_object = target_model[6]#.replace('hd_','').replace('_' + source_system_short,'').replace('_s','').replace('_ms','').replace('_l','').replace('_h','')
    #print('target_model[0][-2:]: ' +  target_model[0].replace('hd_','') + "->" +  target_model[1] + "<-" + target_model[0][-2:] + "/" + target_model[0][-3:]) 
    if target_model[0][-2:] in ('_s') or target_model[0][-3:] in ('_ms'):
      satellite_dict[target_model[0].replace('hd_','')] = target_model[1]
      satellites_dict[target_business_object] = satellite_dict
  for target_model in target_model_list:
    source_system_short = target_model[5]
    target_business_object = target_model[6]#.replace('hk_','').replace('_' + source_system_short,'').replace('_s','').replace('_ms','').replace('_l','').replace('_h','')
    if target_model[0][-2:] in ('_nhl','_l','_h') or target_model[0][-4:] in ('_nhl') :
      target_model_def = target_model_def + "\t" + target_model[0].replace('hk_','') + ":\n"
      target_model_def = target_model_def + "\t\tbusiness_object:\n"
      bk_list = target_model[1].split(",")
      for bk in bk_list:
        if target_model[0][-2:] == '_h':
          target_model_def = target_model_def + "\t\t\t" + "- " + target_model[6].replace("hk_","").replace("_h","").replace("_l","").replace("_nhl","") + ": " + bk + "\n"
        elif target_model[0][-4:] == '_nhl':
          if bk[-3:] == '_bk':
            target_model_def = target_model_def + "\t\t\t" + "- " + bk.replace('_bk',"") + ": " + "hk_" + bk.replace("_bk","") + "_h" + "\n"
        else:
          target_model_def = target_model_def + "\t\t\t" + "- " + bk.replace('_bk',"") + ": " + "hk_" + bk.replace("_bk","") + "_h" + "\n"
      target_model_def = target_model_def + "\t\tsatellites:\n"
      satellite_dict = satellites_dict.get(target_business_object)
      if not satellite_dict is None:
        for sat in satellite_dict:
          attribute_list = satellite_dict[sat].split(",")
          target_model_def = target_model_def + "\t\t\t" + str(sat) + ":\n"
          target_model_def = target_model_def + "\t\t\t\tcolumns:\n"

          for attribute in attribute_list:
            target_model_def = target_model_def + "\t\t\t\t\t" + "- " + attribute + "\n"
      #print('source: ' +  source + "->" +  "object_identifier" + "<-" + target_business_object) 
      sts_satellite_list = generate_status_tracking_satellite_list(cursor=cursor, source=source, object_identifier=target_business_object)
      for sts_sat in sts_satellite_list:
        target_model_def = target_model_def + "\t\t\t" + sts_sat[0]+ ":\n"

  return target_model_def

def gen_multi_active_config(cursor,source):
  
  command = ""

  source_name, source_object = helper.source_split(source)

  query = f"""
              SELECT 
                  s.source_table_identifier
                  ,s.target_satellite_table_physical_name 
                  , s.hub_primary_key_physical_name 
                  , group_concat(s.target_column_physical_name) target_column_physical_name
              FROM hub_satellites s
              inner join source_data src 
                on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' 
                and src.Source_Object = '{source_object}'
                and  ma_attribute
              """
  cursor.execute(query)
  results = cursor.fetchall()
  # print(results)
  if not results:
    # print("not:",results)
    return ""
  command = ""

  for multi_active_config in results:
    if any(item is None for item in multi_active_config):
        continue    
    command += "multi_active_config:\n\t\tmulti_active_key:\n"    
    main_hashkey_column = multi_active_config[2]
    multi_active_key_list = multi_active_config[3].split(",")


    for multi_active_key in multi_active_key_list:
      command += f"\t\t\t- {multi_active_key}\n"  

    command +=  f"\t\tmain_hashkey_column: {main_hashkey_column}\n"
    
  # print(command)
  
  return command




def gen_derived_columns(cursor,source):
  
  command = ""

  source_name, source_object = helper.source_split(source)

  query = f"""
   SELECT 
              group_concat(source_column_physical_name), target_column_physical_name, transformation_rule  
              from
              (              
              SELECT 
              case when transformation_rule<>''
                     then transformation_rule
                     else source_column_physical_name end as source_column_physical_name 
              , target_column_physical_name
              , case when transformation_rule<>''
                     then True
                     else False end as transformation_rule
              FROM hub_satellites s
              inner join source_data src on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              and source_column_physical_name<>target_column_physical_name 
              union
              SELECT 
              source_column_physical_name 
              , target_column_physical_name  
              , False as transformation_rule              
              FROM link_satellites s
              inner join source_data src on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              and source_column_physical_name<>target_column_physical_name
              union
              SELECT distinct
              case when transformation_rule<>''
                     then transformation_rule
                     else source_column_physical_name end as source_column_physical_name
              , business_key_physical_name 
              , case when transformation_rule<>''
                     then True
                     else False end as transformation_rule
              FROM hub_entities s
              inner join source_data src on s.Source_Table_Identifier = src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              and source_column_physical_name<>business_key_physical_name          
              )
              group by target_column_physical_name  
              """
  cursor.execute(query)
  results = cursor.fetchall()
  for derived_columns in results:
    target_column_name = derived_columns[1]
    transformation_rule = derived_columns[2]
    command = command + f"\t\t{target_column_name}:\n"
    if transformation_rule:
      source_column_list = derived_columns[0]
      command = command + f"\t\t\tvalue: {source_column_list}"      
    else:
      source_column_list = derived_columns[0].split(",")
      for i, source_column in enumerate(source_column_list):
          if i == 0:
            command = command + f"\t\t\tvalue: {source_column}"
          else:
            command += f"||'_'||{source_column}"
    command = command + f"\n\t\t\tdatatype: 'VARCHAR'\n"
  return command


def gen_prejoin_columns(cursor, source):
  
  command = ""  

  source_name, source_object = helper.source_split(source)
  
  query = f"""SELECT 
              COALESCE(l.Prejoin_Target_Column_Alias,l.Prejoin_Extraction_Column_Name) as Prejoin_Target_Column_Name,
              pj_src.Source_Schema_Physical_Name, 
              pj_src.Source_Table_Physical_Name,
              l.Prejoin_Extraction_Column_Name, 
              l.Source_column_physical_name,
              l.Prejoin_Table_Column_Name
              FROM link_entities l
              inner join source_data src on l.Source_Table_Identifier = src.Source_table_identifier
              inner join source_data pj_src on l.Prejoin_Table_Identifier = pj_src.Source_table_identifier
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              and l.Prejoin_Table_Identifier is not NULL"""
  
  
  cursor.execute(query)
  prejoined_column_rows = cursor.fetchall()
  for prejoined_column in prejoined_column_rows:

    if command == "":
      command = "prejoined_columns:\n"

    schema = prejoined_column[1]
    table = prejoined_column[2]
    alias = prejoined_column[0]
    bk_column = prejoined_column[3]
    this_column_name = prejoined_column[4]
    ref_column_name = prejoined_column[5]

    command = command + f"""\t{alias}:\n\t\tsrc_schema:"{schema}"\n\t\tsrc_table:"{table}"\n\t\tbk:"{bk_column}"\n\t\tthis_column_name:"{this_column_name}"\n\t\tref_column_name:"{ref_column_name}"\n"""

  return command
  

def generate_yeditest(cursor, source,generated_timestamp,stage_default_schema, model_path,hashdiff_naming):

  target_object_model = ""
  target_object_model = gen_target_objects(cursor, source, hashdiff_naming)
  
  multi_active_config = gen_multi_active_config(cursor, source)

  derived_columns = gen_derived_columns(cursor, source)

  prejoins = gen_prejoin_columns(cursor, source)

  source_name, source_object = helper.source_split(source)
  # print(source_name + ':' + source_object)
  
  test_path = model_path.replace("models", "tests").replace("@@entitytype/", "yedi").replace("@@SourceSystem", source_name)

  query = f"""SELECT Source_Schema_Physical_Name,Source_Table_Physical_Name, Record_Source_Column, Load_Date_Column, source_object, load_completeness_type
              FROM source_data src
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'"""
  cursor.execute(query)
  sources = cursor.fetchall()
  for row in sources: #sources usually only has one row
    source_schema_name = row[0]
    source_table_name = row[1] #.replace('_ws_', '_webshop_').replace('_rs_', '_roadshow_')
    target_table_name = row[1].replace('load', 'yedi') 
    rs = row[2]
    ldts = row[3]
    timestamp = generated_timestamp
    business_object = row[4]
    load_completeness_type = row[5]
    condition = ""

    with open(os.path.join(".","templates","yeditest.txt"),"r") as f:
        command_tmp = f.read()
    f.close()
    command = command_tmp#.replace("@@RecordSource",rs).replace("@@LoadDate",ldts).replace("@@HashedColumns", hashed_columns).replace("@@MultiActiveConfig", multi_active_config).replace("@@derived_columns", derived_columns).replace("@@PrejoinedColumns",prejoins).replace('@@SourceName',source_schema_name).replace('@@SCHEMA',stage_default_schema)
    command = command.replace("@@load_completeness_type", load_completeness_type)
    command = command.replace("@@target_object_model", target_object_model)
    command = command.replace('@@SourceTable',source_table_name)
    filename = os.path.join(test_path, f"{target_table_name.lower()}.sql")

    

    # Check whether the specified path exists or not
    isExist = os.path.exists(test_path)
    if not isExist:   
    # Create a new directory because it does not exist 
        os.makedirs(test_path)

    with open(filename, 'w') as f:
      f.write(command.expandtabs(2))

    print(f"Created yeditest \'{target_table_name.lower()}.sql\'")
