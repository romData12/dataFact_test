from .test import run_query,DB_CONN



df=run_query()
df['add_d']=123

df.to_sql('DATATEST_',DB_CONN,index=False,if_exists='replace')