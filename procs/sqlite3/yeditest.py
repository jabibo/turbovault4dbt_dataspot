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

  

def generate_yeditest(cursor, source,generated_timestamp,stage_default_schema, model_path,hashdiff_naming):

  target_object_model = ""
  target_object_model = gen_target_objects(cursor, source, hashdiff_naming)
  
  source_name, source_object = helper.source_split(source)
  # print(source_name + ':' + source_object)
  
  test_path = model_path.replace("models", "tests").replace("@@entitytype/", "yedi").replace("@@SourceSystem", source_name)

  query = f"""SELECT Source_Schema_Physical_Name,Source_Table_Physical_Name, Record_Source_Column, Load_Date_Column, source_object, load_completeness_type
              FROM source_data src
              WHERE src.Source_System = '{source_name}' and src.Source_Object = '{source_object}'
              and src.JediTest == 'True'
              """
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
