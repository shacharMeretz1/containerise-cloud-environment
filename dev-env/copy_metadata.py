import logging
import os
from sqlalchemy.inspection import inspect
from sqlalchemy import create_engine
import sqlalchemy.orm as orm
from sqlalchemy.orm import Session
import psycopg2 as pg2
from handle_metadata import DBConnector
logger = logging.getLogger()
logger.setLevel(logging.INFO)

forgein_keys_arr={}

def recursive_func(session,inspector,table_name,ids_arr=None):
    forgein_keys=inspector.get_foreign_keys(table_name=table_name)
    
    if len(forgein_keys)==0:
        return

    for forgein_key in forgein_keys: ##all the relation to this class
        all_forgein_keys=[]
        forgein_table_name=forgein_key["referred_table"]
        if forgein_keys_arr.get(forgein_table_name) is None:
                forgein_keys_arr[forgein_table_name]=[]
        local_column=forgein_key["constrained_columns"][0]
        forgein_Table_column=forgein_key["referred_columns"][0]
        for obj_id in ids_arr: ##all the object in this class
            forgein_key_ids = session.execute(f"SELECT {local_column} FROM {table_name} WHERE id = {obj_id}").all()[0]
            if forgein_key_ids[0] is None:
                pass
            elif not isinstance(forgein_key_ids,list):
                all_forgein_keys.append(forgein_key_ids[0])
                if forgein_key_ids[0] not in forgein_keys_arr[forgein_table_name]:
                    forgein_keys_arr[forgein_table_name].append(forgein_key_ids[0])
                recursive_func(session,inspector,forgein_table_name,all_forgein_keys)
            else:
                for forgein_key_id in forgein_key_ids:
                    all_forgein_keys.append(forgein_key_id[0])
                    if forgein_key_id[0] not in forgein_keys_arr[forgein_table_name]:
                        forgein_keys_arr[forgein_table_name].append(forgein_key_id[0])
                recursive_func(session,inspector,forgein_table_name, all_forgein_keys)
        

def call_recursive(db_uri, measurements_id):
    from_engine = create_engine(db_uri)
    inspector = inspect(from_engine) 
    with orm.Session(from_engine) as from_session:
        eeg_samples = from_session.execute(f"SELECT id FROM eeg_sample where measurement_id = '{measurements_id[0]}'").all()
        eeg_samples_ids=[]
        for eeg_sample in eeg_samples:
            if forgein_keys_arr.get('eeg_sample') is None:
                forgein_keys_arr['eeg_sample']=[]
            forgein_keys_arr['eeg_sample'].append(eeg_sample[0])
            eeg_samples_ids.append(eeg_sample[0])
        recursive_func(from_session,inspector, "eeg_sample", eeg_samples_ids)


def metadata_copy(measurements_id):

    try:
        call_recursive(PROD_DB_URI, measurements_id)
        
        OBJECTS=[]

        conn_prod = DBConnector(
                ssh_host=PROD_TUNNEL_HOST, db_name=PROD_DB, db_user=PROD_USER, db_password=PROD_DB_PASSWORD, db_host=PROD_DB_HOST, db_port=PROD_PORT)
        conn_prod.connect()
        cur_prod=conn_prod.connection.cursor()

        for val in forgein_keys_arr:
            if not len(forgein_keys_arr[val])==0:
                cur_prod.execute(f"SELECT * FROM {val} where id = ANY(ARRAY{forgein_keys_arr[val]})")
                conn_prod.connection.commit()
                full_objects=cur_prod.fetchall()
                for object in full_objects:
                    OBJECTS.append((val,object))

        conn = DBConnector(
                ssh_host=STAGING_TUNNEL_HOST, db_name=STAGING_DB, db_user=STAGING_USER, db_password=STAGING_DB_PASSWORD, db_host=STAGING_DB_HOST, db_port=STAGING_PORT)
        conn.connect()
        cur=conn.connection.cursor()
        for table_name, object in OBJECTS:
                text=f"INSERT INTO {val}"
                cur.execute(f"INSERT INTO {table_name} VALUES %s " , (object,))
                conn.connection.commit()
                result=cur.fetchall()

    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    measurement_ids=["KPZuZmzN"]
    response = metadata_copy(measurement_ids)
