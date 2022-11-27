

from handle_timescale import TimescaleConnector
from sqlalchemy.inspection import inspect
import logging
import os
from sshtunnel import SSHTunnelForwarder

from sqlalchemy import create_engine
import sqlalchemy.orm as orm
from sqlalchemy.orm import Session
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)


DB_HOST = "bs-prod-a404aca-brain-prod.a.timescaledb.io"
PORT = 24140
DB = "braindb"
USER = "eegsense"
SSH_HOST = "18.200.14.25"


# staging
DB_HOST_LOCAL = "bs-staging-brain-6b8a.a.timescaledb.io"
PORT_LOCAL = 24140
DB_LOCAL = "braindb"
USER_LOCAL = "eeg_staging"
SSH_HOST_LOCAL="3.248.161.233"

def create_engine_local():
    tunnel= SSHTunnelForwarder(
                (SSH_HOST_LOCAL, 22), ssh_username="ubuntu", remote_bind_address=(DB_HOST_LOCAL, PORT_LOCAL))
    tunnel.start()
    db_port = tunnel.local_bind_port
    db_host = tunnel.local_bind_host
    engine = create_engine(DB_URI_LOCAL.format(db_port=db_port, db_host=db_host))
    return engine

def insert_df_by_chunks(df,chunk_row_size,engine,table_name,schema_name):
    list_df = [df[i:i+chunk_row_size] for i in range(0,df.shape[0],chunk_row_size)]
    for val in list_df:
        val.to_sql(table_name, engine, schema=schema_name, if_exists="append", index=False)

def timescale_copy(measurements_id):
    try:
        for measurement_id in measurements_id:

            from_tscale = TimescaleConnector(db_host=DB_HOST, db_port=PORT, db_name=DB,
                                             db_user=USER, db_password=DB_PASSWORD, ssh_host=SSH_HOST, ssh_port=22)
            from_tscale.connect()
            sample_id=from_tscale.execute(f"select id from eeg_sample where measurement_id = '{measurement_id}'")[0]
            
            eeg_data_df = from_tscale.get_data_for_sample_id(sample_id,"eeg_data", limit=10)
            aux_data_df = from_tscale.get_data_for_sample_id(sample_id,"aux_data", limit=1)

            engine=create_engine_local()
            
            chunk_size=2
            insert_df_by_chunks(eeg_data_df,chunk_size,engine,"eeg_data","public")
            insert_df_by_chunks(aux_data_df,chunk_size,engine,"aux_data","public")


    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    measurements_id = ["8vhwNyFM"]
    response = timescale_copy(measurements_id)
