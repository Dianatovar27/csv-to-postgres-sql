import pandas as pd
from sqlalchemy import create_engine

db_user="xxxxxx"
db_password="xxxxxx!"
db_host="xxxxx"
db_port="xxxxx"
db_name="xxxxx"
table_name="xxxx"


engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

df = pd.read_csv('Staff_List_Report_09-25-2024.csv')
df.columns = df.columns.str.lower().str.replace('"', '').str.replace('name', 'full_name')
print(df.head())
cols_to_load = ['full_name']
df = df[cols_to_load]
df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
