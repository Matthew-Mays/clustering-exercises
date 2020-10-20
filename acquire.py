import pandas as pd
import numpy as np
from env import user, password, host

def get_connection(db, user=user, host=host, password=password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def acquire_zillow_data():
    sql_query = '''
            SELECT * FROM properties_2017
            JOIN predictions_2017 USING(parcelid)
            LEFT JOIN airconditioningtype USING(airconditioningtypeid)
            LEFT JOIN architecturalstyletype USING(architecturalstyletypeid)
            LEFT JOIN buildingclasstype USING(buildingclasstypeid)
            LEFT JOIN heatingorsystemtype USING(heatingorsystemtypeid)
            LEFT JOIN propertylandusetype USING(propertylandusetypeid)
            LEFT JOIN storytype USING(storytypeid)
            LEFT JOIN typeconstructiontype USING(typeconstructiontypeid)
            LEFT JOIN unique_properties USING(parcelid)
            WHERE latitude IS NOT null AND longitude IS NOT null;
            '''
    df = pd.read_sql(sql_query, get_connection('zillow'))
    return df

def rows_missing(df):
    missing_row_percent = (df.isnull().sum() / len(df)) * 100
    missing_row_raw = df.isnull().sum()
    missing_df = pd.DataFrame({'num_rows_missing' : missing_row_raw, 'pct_rows_missing' : missing_row_percent})
    return missing_df

def cols_missing(df):
    num_cols_missing = df.isnull().sum(axis=1)
    pct_cols_missing = df.isnull().sum(axis=1)/df.shape[1]*100
    missing_cols_df = pd.DataFrame({'num_cols_missing': num_cols_missing, 'pct_cols_missing': pct_cols_missing}).reset_index().groupby(['num_cols_missing','pct_cols_missing']).count().rename(index=str, columns={'index': 'num_rows'}).reset_index()
    return missing_cols_df