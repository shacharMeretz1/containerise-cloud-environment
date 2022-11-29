from sqlalchemy.inspection import inspect
import logging
import os

from sqlalchemy import create_engine
import sqlalchemy.orm as orm
from sqlalchemy.orm import Session
import psycopg2 as pg2
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#from common_models import EEGSample, EEGSubject, SubjectPII

ARR={}
already_check=[]
def recursive_func(inspector,obj_name,obj_arr=None):
    already_check.append(obj_name)
    #forgein_keys=inspector.get_foreign_keys(table_name=table_name)
    relations = inspect(obj_name).relationships.items()
    if len(relations)==0:
        return

    for relation in relations: ##all the relation to this class
        all_relation=[]
        for obj in obj_arr: ##all the object in this class
            objs=getattr(obj,relation[0])
            if not isinstance(objs,list):
                all_relation.append(objs)
                if objs not in ARR:
                    ARR.append(objs)
                if objs.__class__ not in already_check:
                    recursive_func(inspector,objs.__class__,all_relation)
            else:
                for obj in objs:
                    all_relation.append(obj)
                    if obj not in ARR:
                        ARR.append(obj)
                if objs.__class__ not in already_check:
                    recursive_func(inspector,obj.__class__,all_relation)
        
        


    '''for relation in eeg_subject_relations:
        rel=relation[0]
        val=getattr(result,rel)
    if len(eeg_subject_relations)==0:
        return

    for index in range(0,len(forgein_keys)):
        recursive_func(inspector, forgein_keys[index]['referred_table'])
        forgein_keys_dict=forgein_keys[index]

        print("x")               
        if table not in models:
            models.append(table)
                            
        referred_table=forgein_keys_dict['referred_table']
        if referred_table not in models:
            models.append(referred_table)

        column=forgein_keys_dict['constrained_columns'][0]
        referred_column=forgein_keys_dict['referred_columns'][0]
        joins.append([table,referred_table,f"{table}.{column} = {referred_table}.{referred_column}"])'''

def recursive_func2(session,inspector,table_name,ids_arr=None):
    forgein_keys=inspector.get_foreign_keys(table_name=table_name)
    #relations = inspect(obj_name).relationships.items()

    if len(forgein_keys)==0:
        return

    for forgein_key in forgein_keys: ##all the relation to this class
        all_forgein_keys=[]
        forgein_table_name=forgein_key["referred_table"]
        if ARR.get(forgein_table_name) is None:
                ARR[forgein_table_name]=[]
        local_column=forgein_key["constrained_columns"][0]
        forgein_Table_column=forgein_key["referred_columns"][0]
        for obj_id in ids_arr: ##all the object in this class
            #temp=forgein_key["constrained_columns"][0]
            
            rs = session.execute(f"SELECT {local_column} FROM {table_name} where id = {obj_id}").all()
            rs=rs[0]
            
            #objs=getattr(obj,temp)
            if rs[0] is None:
                pass
            elif not isinstance(rs,list):
                all_forgein_keys.append(rs[0])
                if rs[0] not in ARR[forgein_table_name]:
                    ARR[forgein_table_name].append(rs[0])
                recursive_func2(session,inspector,forgein_table_name,all_forgein_keys)
            else:
                for result in rs:
                    all_forgein_keys.append(result[0])
                    if result[0] not in ARR[forgein_table_name]:
                        ARR[forgein_table_name].append(result[0])
                recursive_func2(session,inspector,forgein_table_name, all_forgein_keys)
        

def call_recursive(measurements_id):
    from_db_uri = os.environ.get("DB_URI")
    from_engine = create_engine(from_db_uri)
    inspector = inspect(from_engine) 
    with orm.Session(from_engine) as from_session:
        eeg_samples = from_session.execute(f"SELECT id FROM eeg_sample where measurement_id = '{measurements_id[0]}'").all()
        #eeg_samples = from_session.query(EEGSample).filter(EEGSample.measurement_id == measurements_id[0]).all()
        eeg_samples_ids=[]
        for eeg_sample in eeg_samples:
            if ARR.get('eeg_sample') is None:
                ARR['eeg_sample']=[]
            ARR['eeg_sample'].append(eeg_sample[0])
            eeg_samples_ids.append(eeg_sample[0])
        #recursive_func(inspector, EEGSample, eeg_samples)
        recursive_func2(from_session,inspector, "eeg_sample", eeg_samples_ids)
def metadata_copy(measurements_id):

    ###Way to get all the relationships that exist for one type of object) - need it for later
    '''eeg_subject_relations = inspect(EEGSample).relationships.items()
    for relation in eeg_subject_relations:
        print(relation[0])
        print (relation[1])'''
    

    try:
        call_recursive(measurements_id)

        
        DB_HOST = "localhost"
        PORT = 5432
        DB = "postgres"
        USER = "shachar"
        DB_PASSWORD="shachar"
        conn = pg2.connect(
                dbname=DB, user=USER, password=DB_PASSWORD, host=DB_HOST, port=PORT)
        cur = conn.cursor()
        for val in ARR:
            print(val)
            if not len(ARR[val])==0:
                cur.execute(f"SELECT * FROM {val} where id = ANY(ARRAY{ARR[val]})")
                conn.commit()
                data=cur.fetchall()
                for object in data:
                    text=f"INSERT INTO {val}"
                    cur.execute(f"INSERT INTO {val} VALUES %s " , ( object,))
                    conn.commit()
                    result=cur.fetchall()
        #conn.commit()
        cur.close()

        #from_db_uri = os.environ.get("DB_URI")
        #from_engine = create_engine(from_db_uri)
        #with Session(from_engine) as from_session:
            
            #for val in ARR:
            #  objects = from_session.execute(f"SELECT * FROM {val} where id = ANY(ARRAY{ARR[val]})").all()
            #   for object in objects:
            #      stmt="""INSERT INTO :val VALUES :val2 """
            #      data=(val, str(object))
            #      curs=from_session.connection().connection.cursor()
            #      print (curs.mogrify(stmt, data))
            #    rs=from_session.execute(stmt,data)

        '''inspector = inspect(from_engine) 
            models = []
            joins = []
            for table in inspector.get_table_names():
                if not table == "eeg_data":
                    forgein_keys=inspector.get_foreign_keys(table_name=table)
                    if len(forgein_keys)>0:
                        for index in range(0,len(forgein_keys)):
                            forgein_keys_dict=forgein_keys[index]
                            
                            if table not in models:
                                models.append(table)
                            
                            referred_table=forgein_keys_dict['referred_table']
                            if referred_table not in models:
                                models.append(referred_table)

                            column=forgein_keys_dict['constrained_columns'][0]
                            referred_column=forgein_keys_dict['referred_columns'][0]
                            joins.append([table,referred_table,f"{table}.{column} = {referred_table}.{referred_column}"])


                            q = from_session.query(*models)
                            for join_args in joins:
                                q = q.join(*join_args)
                            q=q.filter(EEGSample.measurement_id.in_(measurements_id)).all()
                            print(q)

                            print(forgein_keys_dict)
                            print(f"table: {table}")
                            print(f"column: {forgein_keys_dict['constrained_columns']}")
                            print(f"referred_table: {forgein_keys_dict['referred_table']}")
                            print(f"referred_columns: {forgein_keys_dict['referred_columns']}")
                            print("\n")


            object_arr=[]
            stmt = from_session.query(**models)
            stmt=result.split("stmt")[0]
            stmt+=f" FROM {models[0]} JOIN {models[1]} ON {joins[0][3]}"
            #for join in joins:
                #stmt+=
            result=f"{result} subject_pii JOIN eeg_subject ON eeg_subject.pii_id = subject_pii.id JOIN eeg_sample ON eeg_sample.subject_id = eeg_subject.id "
                #).join(EEGSubject,EEGSubject.pii_id == SubjectPII.id
                #).join(EEGSample,  EEGSample.subject_id == EEGSubject.id
                #).filter(EEGSample.measurement_id.in_(measurements_id)
                #)
            
            print(result)

            for row in result:
                for db_object in row:
                    if db_object not in object_arr:
                        object_arr.append(db_object)
                        from_session.expunge(db_object)

        to_db_uri = os.environ.get("DB_URI2")
        to_engine = create_engine(to_db_uri)
        with orm.Session(to_engine) as to_session:
            for obj in object_arr:
                to_session.merge(obj)
            to_session.add_all(object_arr)
            to_session.commit()'''

            
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    measurement_ids=["9b19d4cd-f84a-47d9-81ed-487e3ae2436b"]
    response = metadata_copy(measurement_ids)
