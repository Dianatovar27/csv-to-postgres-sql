import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import re

db_user = 'xxxx'
db_password = 'xxxx'
db_host = 'xxxx'
db_port = 'xxxx'
db_name = 'xxxx'

engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

dfclasses = pd.read_csv('class_list.csv')
dfcamp= pd.read_csv('class_camp.csv')
dfcoaches = pd.read_sql("SELECT coach_id, full_name FROM coaches", engine)

dfclasses = pd.merge(
    dfclasses,
    dfcamp[['Instructors', 'Event Name', 'Student Name', 'Class Enrollment Count', 'Camp Enrollment Count', 'Active Enrollment Count']],
    left_on=['Instructors', 'Class Name'],
    right_on=['Instructors', 'Event Name'],
    how='left'
)

dfclasses.columns = dfclasses.columns.str.lower().str.replace('"', '')

#print(dfclasses.head(10))

#coah id 
coach_id_mapping = pd.Series(dfcoaches.coach_id.values, index=dfcoaches.full_name).to_dict()

def normalize_and_split_names(name_str):
    if pd.isna(name_str):
        return []
    name_str = re.sub(r'\s+', ' ', name_str).strip()
    name_parts = name_str.split(' ')
    return [' '.join(name_parts[i:i + 2]) for i in range(0, len(name_parts), 2)]

def map_names_to_ids(name_pairs):
    ids = [str(coach_id_mapping.get(pair, 'Unknown')) for pair in name_pairs]
    return ','.join(filter(lambda x: x != 'Unknown', ids))

def process_instructors(instructor_names):
    if pd.isna(instructor_names):
        return ''
    name_pairs = normalize_and_split_names(instructor_names)
    coach_id = map_names_to_ids(name_pairs)
    return coach_id

dfclasses['instructors'] = dfclasses['instructors'].apply(process_instructors)

#print(dfclasses['instructors'].head(10))

def convert_coach_id_to_list(ids):
    if pd.isna(ids):
        return []
    ids_list = [int(id.strip()) for id in ids.split(',') if id.strip().isdigit()]
    return ids_list

dfclasses['coach_id'] = dfclasses['instructors'].apply(convert_coach_id_to_list)

#print(dfclasses['coach_id'].head(10))

dfclasses['coach_id'] = dfclasses['coach_id'].apply(lambda x: x if isinstance(x, list) else [])

#print(dfclasses['coach_id'].head(10))

def explode_coach_id(df):
    df_exploded = df.explode('coach_id')
    df_exploded['coach_id'] = df_exploded['coach_id'].fillna(0).astype(int)
    return df_exploded

dfclasses_exploded = explode_coach_id(dfclasses)
#end of coach id

#print(dfclasses.head(10))

print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
dfclasses.to_csv('aqui.csv', index=False)
print(dfclasses.dtypes)
print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')


#column class_type
def class_or_camp(x):
    try:
        value = int(x)
        if 1 <= value <= 20:
            return 1
        else:
            return 0
    except ValueError:
        return 0

dfclasses['class enrollment count'] = dfclasses['class enrollment count'].apply(class_or_camp)
dfclasses['camp enrollment count'] = dfclasses['camp enrollment count'].apply(class_or_camp)

def determine_class_type(row):
    if row['class enrollment count'] == 1:
        return 'class'
    elif row['camp enrollment count'] == 1:
        return 'camp'
    else:
        return 'none' 

dfclasses['class_type'] = dfclasses.apply(determine_class_type, axis=1)

#colum weekday and start_time
weekday_mapping = {
    'Sun': 'Sunday',
    'Mon': 'Monday',
    'Tue': 'Tuesday',
    'Wed': 'Wednesday',
    'Thu': 'Thursday',
    'Fri': 'Friday',
    'Sat': 'Saturday'
}

def extract_schedule_info(schedule):
    schedule = str(schedule) if not pd.isna(schedule) else ''
    
    weekday = None
    start_time = None 
    
    try:
        parts = schedule.split(' - ')
        
        if len(parts) == 2:
            weekday_abbr = parts[0].strip()
            weekday = weekday_mapping.get(weekday_abbr, None )
            
            time_part = parts[1].split('-')[0].strip()
            start_time = time_part
        
    except (IndexError, ValueError) as e:
        print(f"Error processing schedule '{schedule}': {e}")

    return pd.Series([weekday, start_time])

dfclasses[['weekday', 'start_time']] = dfclasses['schedule'].apply(extract_schedule_info)

dfclasses['weekday'] = dfclasses['weekday'].replace('None', None)
dfclasses['start_time'] = dfclasses['start_time'].astype(str)

dfclasses = dfclasses[dfclasses['weekday'].notna()] #***************text*****

#dfclasses.to_csv('dfclasses_output8.csv', index=False)

dfclasses['weekday'] = dfclasses['weekday'].astype(str)
dfclasses['start_time'] = dfclasses['start_time'].astype(str)

def convert_to_24_hour(time_str):
    try:
        return datetime.strptime(time_str, '%I:%M%p').strftime('%H:%M:%S')
    except ValueError:
        return '00:00:00'
    
dfclasses['start_time'] = dfclasses['start_time'].apply(convert_to_24_hour)

#column duration minutes
duration_mapping = {
    'class': 55,
    'camp': 180
}

dfclasses['duration_minutes'] = dfclasses['class_type'].map(duration_mapping)

dfclasses['duration_minutes'] = dfclasses['duration_minutes'].fillna(0).astype(int)

dfclasses = dfclasses.rename(columns={
    'class name': 'class_name',
    'instructors': 'coach_id'
})

#column age_range
def convert_age_range(age_range_str):
    try:
        parts = age_range_str.split('-')
        lower_bound = parts[0].strip()
        upper_bound = parts[1].strip() if len(parts) > 1 else ''
        
        if not upper_bound:
            upper_bound = float('inf')
            
        return f'[{lower_bound},{upper_bound})'
    except Exception as e:
        print(f"Error processing '{age_range_str}': {e}")
        return None

dfclasses['age_range'] = dfclasses['age range'].apply(convert_age_range)


#dfclasses.to_csv('dfclasses_output2.csv', index=False)
#print(dfclasses.dtypes)

cols_to_load = ['class_name', 'class_type', 'weekday' , 'start_time', 'duration_minutes' , 'age_range']
dfclasses = dfclasses[cols_to_load]
#print(dfclasses.columns)
#print(dfclasses.dtypes)
#print(dfclasses.head(10))
#print(dfclasses_exploded.head(10))
#print(dfclasses_exploded.dtypes)
def insert_into_classes(df, engine):
    df.to_sql(name='classes', con=engine, if_exists='append', index=False)

#################################################insert_into_classes(dfclasses, engine) aqui mande a sql

############

query = "SELECT class_id, class_name FROM classes"
df_classes_coaches = pd.read_sql(query, engine)

dfclasses_exploded = dfclasses_exploded.rename(columns={'class name': 'class_name'})

df_classes_coaches = pd.merge(
    df_classes_coaches,
    dfclasses_exploded[['class_name', 'coach_id']],
    on='class_name',
    how='left')

df_classes_coaches = df_classes_coaches.dropna(subset=['class_id'])
df_classes_coaches = df_classes_coaches[(df_classes_coaches['coach_id'] != 0) & (df_classes_coaches['coach_id'].notna())]
df_classes_coaches = df_classes_coaches[['class_id', 'coach_id']]
df_classes_coaches = df_classes_coaches.drop_duplicates()

df_classes_coaches.to_csv('miiraaa.csv', index=False)
print(df_classes_coaches.dtypes)

###############df_classes_coaches.to_sql(name='class_coaches', con=engine, if_exists='append', index=False)  aqui mande a sql


## cambios: una tabla solo para clases, una table solo para camps 



