import os
from configparser import ConfigParser
from procs.sqlite3 import stage
from procs.sqlite3 import satellite
from procs.sqlite3 import hub
from procs.sqlite3 import link
from procs.sqlite3 import nh_link
from procs.sqlite3 import load
from procs.sqlite3 import landing_zone
						  
import pandas as pd
import sqlite3
from logging import Logger
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime
import time
import argparse

image_path = os.path.join(os.path.dirname(__file__), "images")


def connect_sqlite():
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "config.ini"))
    db_path = config.get("sqlite3", "db_path")
    conn = sqlite3.connect(os.path.join(db_path, "dataspotparameters.db"))
    cursor = conn.cursor()

    sql_source_data = "SELECT * FROM source_data"
    df_source_data = pd.read_sql_query(sql_source_data, conn)

    sql_hub_entities = "SELECT * FROM hub_entities"
    df_hub_entities = pd.read_sql_query(sql_hub_entities, conn)

    sql_hub_satellites = "SELECT * FROM hub_satellites"
    df_hub_satellites = pd.read_sql_query(sql_hub_satellites, conn)

    sql_link_entities = "SELECT * FROM link_entities"
    df_link_entities = pd.read_sql_query(sql_link_entities, conn)

    sql_link_satellites = "SELECT * FROM link_satellites"
    df_link_satellites = pd.read_sql_query(sql_link_satellites, conn)

    sql_nh_link_entities = "SELECT * FROM nh_link_entities"
    df_nh_link_entities = pd.read_sql_query(sql_nh_link_entities, conn)

    sql_landing_zone= "SELECT * FROM landing_zone"
    df_landing_zone = pd.read_sql_query(sql_landing_zone, conn)

    sql_load_tables= "SELECT * FROM load_tables"
    df_load_tables = pd.read_sql_query(sql_load_tables, conn)

    sql_load_attributes= "SELECT * FROM load_attributes"
    df_load_attributes = pd.read_sql_query(sql_load_attributes, conn)

    dfs = {
        "source_data": df_source_data,
        "hub_entities": df_hub_entities,
        "link_entities": df_link_entities,
        "hub_satellites": df_hub_satellites,
        "link_satellites": df_link_satellites,
        "nh_link_entities": df_nh_link_entities,        
        "landing_zone": df_landing_zone,        
        "load_tables": df_load_tables,        
        "load_attributes": df_load_attributes,        
    }

    db = sqlite3.connect(':memory:')

    for table, df in dfs.items():
        df.to_sql(table, db)

    sqlite_cursor = db.cursor()

    return sqlite_cursor


@Gooey(
    navigation="TABBED",
    program_name="TurboVault",
    default_size=(800, 800),
    advanced=True,
    image_dir=image_path,
    command_line_args=True
)
def main():
	
    config = ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

    model_path = config.get("sqlite3", "model_path")
    hashdiff_naming = config.get("sqlite3", "hashdiff_naming")

    cursor = connect_sqlite()

    cursor.execute("SELECT DISTINCT SOURCE_SYSTEM || '_' || SOURCE_OBJECT FROM source_data")
    results = cursor.fetchall()
    available_sources = []

    
    for row in results:
        available_sources.append(row[0])

    generated_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Set default values for the arguments
    #default_tasks = ["Stage", "Hub", "Satellite", "Link", "non_historized_Link", "landing_zone"]
    default_tasks = ["load"]
    default_sources = [["webshop_lieferung"]]


    # Set a flag to indicate whether to use Gooey or not
    use_gooey = True

    # Check if the program is running in debug mode
    if not use_gooey:

        # Use default values for the arguments
        class Args:
            Tasks = default_tasks
            Sources = default_sources
        
        args = Args()

    else:

        parser = GooeyParser(description="Config")
        parser.add_argument(
            "--Tasks",
            help="Select the entities which you want to generate",
            action="append",
            widget="Listbox",
            choices=["Stage", "Hub", "Satellite", "Link", "non_historized_Link", "landing_zone", "Load"],
            default=default_tasks,
            nargs="*",
            gooey_options={"height": 300},
        )
        parser.add_argument(
            "--Sources",
            help="Select the sources which you want to process",
            action="append",
            widget="Listbox",
            choices=available_sources,
            nargs="+",        
            gooey_options={"height": 300},
        )
        args = parser.parse_args()
        
        
    try:
        todo = args.Tasks

    except IndexError:
        print("No entities selected.")
        todo = ""

    rdv_default_schema = "rdv"
    stage_default_schema = "stage"

    for source in args.Sources[0]:
        if "Stage" in todo:
            stage.generate_stage(cursor,source, generated_timestamp, stage_default_schema, model_path, hashdiff_naming)
        
        if 'Hub' in todo: 
            hub.generate_hub(cursor,source, generated_timestamp, rdv_default_schema, model_path)
    
        if 'Link' in todo: 
            link.generate_link(cursor,source, generated_timestamp, rdv_default_schema, model_path)

        if 'Satellite' in todo: 
            satellite.generate_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming)

        if 'non_historized_Link' in todo: 
            nh_link.generate_nh_link(cursor,source, generated_timestamp, rdv_default_schema, model_path)

        if 'load' in todo: 
            load.generate_load(cursor, source, model_path)

        if 'landing_zone' in todo: 
            landing_zone.generate_landing_zone(cursor, source, model_path)



    cursor.close()

if __name__ == "__main__":
    print("Starting Script.")
    start = time.time()
    main()
    end = time.time()
    print("Script ends.")
    print("Total Runtime: " + str(round(end - start, 2)) + "s")

