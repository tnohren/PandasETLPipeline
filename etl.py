import pandas as pd
from db import MainDatabase

class ETL():
    def __init__(self):
        self.pop_data = None
        self.unemp_data = None

    def Extract(self):
        """Extract US Government Population and Unemployment Data"""
        self.pop_data = pd.read_csv(r'./static/cbsa-est2017-alldata.csv', encoding = "ISO-8859-1")
        self.unemp_data = pd.read_excel(r'./static/Unemployment.xls', skiprows=7)

    def Transform(self):
        """Transform US Government Population and Unemployment Data"""
        # Clean and Transform USG Population Data
        pop_indexes = ['iMSACode', 'rMDIVCode', 'iSTCOUCode', 'tName', 'tAreaType']
        pop_columns = [column for column in self.pop_data.columns if column.startswith('POPEST')]
        self.pop_data = pd.melt(frame=self.pop_data[pop_indexes + pop_columns],\
                                id_vars=pop_indexes,
                                value_vars=pop_columns,
                                var_name="iYear",
                                value_name="iPopulation")
        self.pop_data['iYear'] = self.pop_data['iYear'].apply(lambda yr: yr[-4:])

        # Clean and Transform USG Unemployment Data
        unemp_indexes = ['iFIPSCode', 'tState', 'tArea']
        unemp_columns = [column for column in self.unemp_data.columns if column.startswith('Unemployment_rate')]
        #self.unemp_data.reindex(columns=unemp_indexes)
        self.unemp_data = pd.melt(frame=self.unemp_data[unemp_indexes + unemp_columns],
                                id_vars = unemp_indexes,
                                value_vars = unemp_columns,
                                var_name='iYear',
                                value_name='rRate').round(1)
        self.unemp_data['iYear'] = self.unemp_data['iYear'].apply(lambda yr: yr[-4:])

    def Load(self):
        main_db = MainDatabase('db/db.sqlite')
        self.pop_data.to_sql('pop', main_db.connection, if_exists='append', index=False)
        self.unemp_data.to_sql('unemp', main_db.connection, if_exists='append', index=False)