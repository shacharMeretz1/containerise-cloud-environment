import logging
from sqlalchemy.inspection import inspect
import sqlalchemy.orm as orm
from handle_metadata import DBConnector,create_engine_local
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
        

def call_recursive(from_engine, measurements_id):
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
        from_engine = create_engine_local(PROD_TUNNEL_HOST,PROD_DB_HOST,PROD_PORT,PROD_DB_URI)

        call_recursive(from_engine,measurements_id)
        
        OBJECTS=[]

        from_conn = from_engine.raw_connection()
        from_cur = from_conn.cursor()

        for val in forgein_keys_arr:
            if not len(forgein_keys_arr[val])==0:
                from_cur.execute(f"SELECT * FROM {val} where id = ANY(ARRAY{forgein_keys_arr[val]})")
                from_conn.connection.commit()
                full_objects=from_cur.fetchall()
                for object in full_objects:
                    OBJECTS.append((val,object))

        conn = DBConnector(
                ssh_host=STAGING_TUNNEL_HOST, db_name=STAGING_DB, db_user=STAGING_USER, db_password=STAGING_DB_PASSWORD, db_host=STAGING_DB_HOST, db_port=STAGING_PORT)
        conn.connect()
        cur=conn.connection.cursor()
        for table_name, object in OBJECTS:
                cur.execute(f"INSERT INTO {table_name} VALUES %s " , (object,))
                conn.connection.commit()
                cur.fetchall()

    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    measurement_ids=["KPZuZmzN"]
    response = metadata_copy(measurement_ids)
