import pandas as pd
from sqlalchemy import create_engine

db_user = 'xxxx'
db_password = 'xxxx'
db_host = 'xxxx'
db_port = 'xxxx'
db_name = 'xxxx'
table_name = 'xxxx'

engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

df = pd.read_csv('students.csv')
df.columns = df.columns.str.lower().str.replace('"', '').str.replace('student', 'full_name')
print(df.head())
cols_to_load = ['full_name', 'birthday', 'address']
df = df[cols_to_load]
df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
