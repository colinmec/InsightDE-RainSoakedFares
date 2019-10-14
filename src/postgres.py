# This module handles connection and writing to postgreSQL
# Author: Colin Chow
# Date created: 09/20/2019
# Version:

import os

from globalVar import getVal as glb # Custom, see globalVar.py
from pyspark.sql import DataFrameWriter
from pyspark.sql import DataFrameReader

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

    def write(self, df, table, mode, **kwargs):
        my_writer = self.get_writer(df)
        if 'db' in kwargs:
            url = "jdbc:postgresql://" + self.hostname + \
                  ":5432/" + kwargs.get('db')
        else:
            url = self.url_connect
        my_writer.jdbc(url, table, mode, self.properties)

    def get_reader(self, spark):
        return DataFrameReader(spark)

    def read(self, spark, table, **kwargs):
        my_reader = self.get_reader(spark)
        if 'db' in kwargs:
            url = "jdbc:postgresql://" + self.hostname + \
                  ":5432/" + kwargs.get('db')
        else:
            url = self.url_connect
        
        if 'numPartitions' in kwargs:
            return my_reader.jdbc(url, table, numPartitions=kwargs.get('numPartitions'), \
                                                     column=kwargs.get('column'),        \
                                                 lowerBound=kwargs.get('lowerBound'),    \
                                                 upperBound=kwargs.get('upperBound'),    \
                                                 properties=self.properties)
        else:
            return my_reader.jdbc(url, table, properties=self.properties)
