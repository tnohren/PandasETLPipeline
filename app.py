from etl import ETL

if __name__ == '__main__':
    pl = ETL()
    pl.Extract()
    pl.Transform()
    pl.Load()