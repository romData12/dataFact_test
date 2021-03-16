from datetime import datetime
import pandas as pd

import urllib
from sqlalchemy import create_engine
from datetime import datetime

import os
basedir = os.path.abspath(os.path.dirname(__file__))




DB_SERVER=os.environ.get('DB_SERVER')
DB_STAGING_DATABASE=os.environ.get('DB_STAGING_DATABASE')
DB_USER=os.environ.get('DB_USER')
DB_USER_PASSWORD=os.environ.get('DB_USER_PASSWORD')
DB_DRIVER=os.environ.get('DB_DRIVER')

DB_TABLE=os.environ.get('DB_TABLE')





params = urllib.parse.quote_plus(f'DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_STAGING_DATABASE};UID={DB_USER};PWD={DB_USER_PASSWORD}')
#params = urllib.parse.quote_plus(f'''DRIVER={DB_DRIVER};SERVER=localhost;DATABASE=SORQA;Trusted_Connection=yes''')
DB_CONN = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params),pool_size=50, max_overflow=10)

def run_query():    
    df=pd.read_sql(f"SELECT * FROM {DB_TABLE}",DB_CONN)

    df['double']=df.iloc[:,1]
    df['create2']=datetime.now()

    df.to_sql('DATATEST',DB_CONN,index=False,if_exists='replace')

    return df


if __name__ == "__main__":
    # execute only if run as a script
    run_query()