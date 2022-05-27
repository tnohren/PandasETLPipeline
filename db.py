import sqlite3 as sql
from sqlite3 import Error

class MainDatabase:
    def __init__(self, db_dir):
        self.db_dir = db_dir
        self.CreateConnection()
        self.BuildTables()

    def CreateConnection(self):
        self.connection = None
        try:
            self.connection = sql.connect(self.db_dir)
        except Error as e:
            print(f"Error: {e}")

    def BuildTables(self):
        pop_table_script = """
            CREATE TABLE IF NOT EXISTS pop(
                iMSACode INTEGER,
                rMDIVCode REAL,
                iSTCOUCode INTEGER,
                tName TEXT,
                tAreaType TEXT,
                iYear INTEGER,
                iPopulation INTEGER
            )"""
        unemp_table_script = """
            CREATE TABLE unemp(
                iFIPSCode INTEGER,
                tState TEXT,
                tArea TEXT,
                iYear INTEGER,
                rRate REAL
            )"""
        self.ExecuteCreate(pop_table_script)
        self.ExecuteCreate(unemp_table_script)

    def ExecuteCreate(self, script):
        cursor = self.connection.cursor()
        try:
            cursor.execute(script)
            self.connection.commit()
        except Error as e:
            self.connection.rollback()
            print(f"Error: {e} during executing: \n {script}")