# This module handles connection and writing to postgreSQL
# Author: Colin Chow
# Date created: 09/20/2019
# Version:

import os

from globalVar import getVal as glb # Custom, see globalVar.py
from pyspark.sql import DataFrameWriter

class PostgresConnector(object):
    def __init__(self):
        self.database_name = glb('dbName')
        self.hostname = glb('pgHost')
        self.username = glb('pgUser')
        self.password = glb('pgPasswd')
        self.url_connect = "jdbc:postgresql://" + self.hostname + \
                           ":5432/" + self.database_name
        self.properties = {"user"     : self.username,
                           "password" : self.password,
                           "driver"   : "org.postgresql.Driver"}

    def get_writer(self, df):
        return DataFrameWriter(df)

    def write(self, df, table, mode):
        my_writer = self.get_writer(df)
        my_writer.jdbc(self.url_connect, table, mode, self.properties)
