from sqlalchemy.inspection import inspect
import logging
import os

from sqlalchemy import create_engine
import sqlalchemy.orm as orm
from sqlalchemy.orm import Session

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from common_models import EEGSample, EEGSubject, SubjectPII

def metadata_copy(measurements_id):

    ###Way to get all the relationships that exist for one type of object) - need it for later
    '''eeg_subject_relations = inspect(EEGSample).relationships.items()
    for relation in eeg_subject_relations:
        print (relation)'''

    try:
        from_db_uri = os.environ.get("DB_URI")
        from_engine = create_engine(from_db_uri)
        with orm.Session(from_engine) as from_session:
    
            object_arr=[]
            result = from_session.query(SubjectPII,EEGSubject,EEGSample
                ).join(EEGSubject,EEGSubject.pii_id == SubjectPII.id
                ).join(EEGSample,  EEGSample.subject_id == EEGSubject.id
                ).filter(EEGSample.measurement_id.in_(measurements_id)
                ).all()

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
            to_session.commit()

            
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    measurement_ids=["9b19d4cd-f84a-47d9-81ed-487e3ae2436b"]
    response = metadata_copy(measurement_ids)
