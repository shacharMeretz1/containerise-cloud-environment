import logging
from sqlalchemy.inspection import inspect
import sqlalchemy.orm as orm
from handle_metadata import DBConnector,create_engine_local
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROD_DB_URI="postgresql://eegsense:AVNS_8wWx1oNQ7FA6FxRnJTI@{db_host}:{db_port}/braindb"
PROD_DB_HOST="bs-prod-a404aca-brain-prod.a.timescaledb.io"
PROD_PORT=24140
PROD_USER="eegsense"
PROD_DB="braindb"
PROD_DB_PASSWORD="AVNS_8wWx1oNQ7FA6FxRnJTI"
PROD_TUNNEL_HOST="18.200.14.25"

STAGING_DB_URI="postgresql://shachar:shachar@localhost:5432/postgres"
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
            

def call_recursive(from_engine):
    inspector = inspect(from_engine) 
    with orm.Session(from_engine) as from_session:
        eeg_samples = from_session.execute(f"SELECT id FROM eeg_sample where measurement_successful=True ORDER BY usage_time ASC LIMIT 10").all()
        eeg_samples_ids=[]
        for eeg_sample in eeg_samples:
            if forgein_keys_arr.get('eeg_sample') is None:
                forgein_keys_arr['eeg_sample']=[]
            forgein_keys_arr['eeg_sample'].append(eeg_sample[0])
            eeg_samples_ids.append(eeg_sample[0])
        recursive_func(from_session,inspector, "eeg_sample", eeg_samples_ids)

def change_json_attribute(index,full_objects):
    for list_index,obj in enumerate(full_objects):
        list_obj=list(obj)
        dict_obj=list_obj[index]
        dict_obj=json.dumps(dict_obj)
        list_obj[index]=dict_obj
        full_objects[list_index]=tuple(list_obj)
    return full_objects


def metadata_copy():
    
    tables=["sensi_event_type",#"analysis_type","analysis_version",
    "measurement_flag_type","question_options"]
                               
    try:
        from_engine = create_engine_local(PROD_TUNNEL_HOST,PROD_DB_HOST,PROD_PORT,PROD_DB_URI)

        call_recursive(from_engine)
        
        OBJECTS=[]
        full_objects=None

        from_conn = from_engine.raw_connection()
        from_cur = from_conn.cursor()

        for val in forgein_keys_arr:
            from_cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{val}';")
            from_conn.connection.commit()
            columns_type=from_cur.fetchall()
            if not len(forgein_keys_arr[val])==0:
                from_cur.execute(f"SELECT * FROM {val} WHERE id = ANY(ARRAY{forgein_keys_arr[val]})")
                from_conn.connection.commit()
                full_objects=from_cur.fetchall()
                index=0
                for _,column_type in columns_type:
                    if column_type=="jsonb" or column_type=="json":
                        full_objects=change_json_attribute(index,full_objects)
                    index+=1
                for obj in full_objects:
                    OBJECTS.append((val,obj))
        
        OBJECTS= OBJECTS[::-1] ##The purpose of this line is to replace the array (we want to add the object in the right order base on the relationship objects)

        for table in tables:
            query=f"SELECT * FROM {table}"
            from_cur.execute(query)
            from_conn.connection.commit()
            full_objects=from_cur.fetchall()
            for obj in full_objects:
                OBJECTS.append((table,obj))

        conn = DBConnector(
                ssh_host=STAGING_TUNNEL_HOST, db_name=STAGING_DB, db_user=STAGING_USER, db_password=STAGING_DB_PASSWORD, db_host=STAGING_DB_HOST, db_port=STAGING_PORT)
        conn.connect()
        cur=conn.connection.cursor()
        for table_name, obj in OBJECTS:
            cur.execute(f"INSERT INTO {table_name} VALUES %s " , (obj,))
        conn.connection.commit()

    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    response = metadata_copy()
