import os

import pandas as pd
from sshtunnel import SSHTunnelForwarder
import psycopg2 as pg2
from sqlalchemy import create_engine


SSH_USERNAME = "ubuntu"


def create_engine_local(ssh_host,host,port,db_uri):
    tunnel= SSHTunnelForwarder(
                (ssh_host, 22), ssh_username="ubuntu", remote_bind_address=(host, port))
    tunnel.start()
    db_port = tunnel.local_bind_port
    db_host = tunnel.local_bind_host
    engine = create_engine(db_uri.format(db_port=db_port, db_host=db_host))
    return engine


class DBConnector:
    def __init__(self, db_host: str = None, db_port: int= None, db_name: str= None, db_user: str= None, db_password: str= None, ssh_host: str= None, ssh_port: int= None, is_aws: bool = None, db_uri: str= None):
        should_ssh = True
        # If we have no information whether we not we are running on AWS try performing a simple check
        if is_aws == None:
            if os.getlogin() == "ubuntu":
                # Likely AWS so
                should_ssh = False
        else:
            should_ssh = not is_aws

        if should_ssh and not ssh_host:
            raise ValueError(
                "Looks like you should be using SSH but no ssh info is provided, if you think this is a mistake explicitly pass is_aws = False")

        self.ssh_host = ssh_host
        self.should_ssh = should_ssh
        self.ssh_port = ssh_port if ssh_port else 22
        self.connection = None
        self.engine = None
        self.tunnel = None
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_uri=db_uri
        
    def connect(self):
        if self.connection is not None:
            return
        db_port = self.db_port
        db_host = self.db_host
        if self.should_ssh:
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port), ssh_username=SSH_USERNAME, remote_bind_address=(self.db_host, self.db_port))
            self.tunnel.start()
            db_port = self.tunnel.local_bind_port
            db_host = self.tunnel.local_bind_host

        if self.db_uri:
            uri=self.db_uri.format(db_port=db_port, db_host=db_host)
            self.engine = create_engine(uri)
        else:
            self.connection = pg2.connect(
                dbname=self.db_name, user=self.db_user, password=self.db_password, host=db_host, port=db_port)
        
    def execute(self, statement):
        curr = self.connection.cursor()

        try:
            curr.execute(statement)
            self.connection.commit()
            return curr.fetchall()
        except pg2.ProgrammingError:
            self.connection.rollback()
            return None
        except Exception as e:
            print(e)
            self.connection.rollback()
            return None
   