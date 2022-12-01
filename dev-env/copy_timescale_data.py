

from handle_timescale import TimescaleConnector
import logging
from sshtunnel import SSHTunnelForwarder
import sqlalchemy.orm as orm
from sqlalchemy import create_engine

logger = logging.getLogger()
logger.setLevel(logging.INFO)



PROD_DB_URI="postgresql://eegsense:AVNS_8wWx1oNQ7FA6FxRnJTI@{db_host}:{db_port}/braindb"
PROD_DB_HOST="bs-prod-a404aca-brain-prod.a.timescaledb.io"
PROD_PORT=24140
PROD_USER="eegsense"
PROD_DB="braindb"
PROD_DB_PASSWORD="AVNS_8wWx1oNQ7FA6FxRnJTI"
PROD_TUNNEL_HOST="18.200.14.25"

STAGING_DB_URI="postgresql://eeg_staging:AVNS_bKaGSpRLKCQ30UvSGQD@{db_host}:{db_port}/braindb"
STAGING_DB_HOST="bs-staging-brain-6b8a.a.timescaledb.io"
STAGING_PORT=24140
STAGING_USER="eeg_staging"
STAGING_DB="braindb"
STAGING_DB_PASSWORD="AVNS_bKaGSpRLKCQ30UvSGQD"
STAGING_TUNNEL_HOST="3.248.161.233"

DB_HOST_LOCAL = "localhost"
PORT_LOCAL = 5432
DB_LOCAL = "postgres"
USER_LOCAL = "shachar"
DB_PASSWORD_LOCAL="shachar"

def create_engine_local(ssh_host,host,port,db_uri):
    tunnel= SSHTunnelForwarder(
                (ssh_host, 22), ssh_username="ubuntu", remote_bind_address=(host, port))
    tunnel.start()
    db_port = tunnel.local_bind_port
    db_host = tunnel.local_bind_host
    engine = create_engine(db_uri.format(db_port=db_port, db_host=db_host))
    return engine

def insert_df_by_chunks(df,chunk_row_size,engine,table_name,schema_name):
    list_df = [df[i:i+chunk_row_size] for i in range(0,df.shape[0],chunk_row_size)]
    for val in list_df:
        val.to_sql(table_name, engine, schema=schema_name, if_exists="append", index=False)

def timescale_copy():
    try:
        from_tscale = TimescaleConnector(db_host=PROD_DB_HOST, db_port=PROD_PORT, db_name=PROD_DB,
                                             db_user=PROD_USER, db_password=PROD_DB_PASSWORD, ssh_host=PROD_TUNNEL_HOST, ssh_port=22)
        from_tscale.connect()

        to_engine=create_engine_local(STAGING_TUNNEL_HOST,STAGING_DB_HOST,STAGING_PORT,STAGING_DB_URI)
        with orm.Session(to_engine) as to_session:
            samples_id = to_session.execute(f"SELECT id FROM eeg_sample").all()
  
        for sample_id in samples_id:
            sample_id=sample_id[0]
            eeg_data_df = from_tscale.get_data_for_sample_id(sample_id,"eeg_data")
            aux_data_df = from_tscale.get_data_for_sample_id(sample_id,"aux_data")

            
            chunk_size=50000
            insert_df_by_chunks(eeg_data_df,chunk_size,to_engine,"eeg_data","public")
            insert_df_by_chunks(aux_data_df,chunk_size,to_engine,"aux_data","public")


    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    #measurements_id = ["8vhwNyFM"]
    response = timescale_copy()
