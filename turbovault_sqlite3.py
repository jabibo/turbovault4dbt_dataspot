import os
from configparser import ConfigParser
from procs.sqlite3 import stage
from procs.sqlite3 import satellite
from procs.sqlite3 import hub
from procs.sqlite3 import link
						  
import pandas as pd
import sqlite3
from logging import Logger
from gooey import Gooey
from gooey import GooeyParser
from datetime import datetime
import time

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

    dfs = {
        "source_data": df_source_data,
        "hub_entities": df_hub_entities,
        "link_entities": df_link_entities,
        "hub_satellites": df_hub_satellites,
        "link_satellites": df_link_satellites,
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

    parser = GooeyParser(description="Config")
    parser.add_argument(
        "--Tasks",
        help="Select the entities which you want to generate",
        action="append",
        widget="Listbox",
        choices=["Stage", "Hub", "Satellite", "Link"],
        default=["Stage", "Hub", "Satellite", "Link"],
        nargs="*",
        gooey_options={"height": 300},
    )
    parser.add_argument(
        "--Sources",
        action="append",
        nargs="+",
        widget="Listbox",
        choices=available_sources,
        gooey_options={"height": 300},
        help="Select the sources which you want to process",
    )
    args = parser.parse_args()

    try:
        todo = args.Tasks[4]

    except IndexError:
        print("No entities selected.")
        todo = ""

    rdv_default_schema = "rdv"
    stage_default_schema = "stage"

    for source in args.Sources[0]:
        print(source)
        if "Stage" in todo:
            stage.generate_stage(cursor,source, generated_timestamp, stage_default_schema, model_path, hashdiff_naming)
        
        if 'Hub' in todo: 
            hub.generate_hub(cursor,source, generated_timestamp, rdv_default_schema, model_path)
    
        if 'Link' in todo: 
            link.generate_link(cursor,source, generated_timestamp, rdv_default_schema, model_path)

        if 'Satellite' in todo: 
            satellite.generate_satellite(cursor, source, generated_timestamp, rdv_default_schema, model_path, hashdiff_naming)

    cursor.close()

if __name__ == "__main__":
    print("Starting Script.")
    start = time.time()
    main()
    end = time.time()
    print("Script ends.")
    print("Total Runtime: " + str(round(end - start, 2)) + "s")
