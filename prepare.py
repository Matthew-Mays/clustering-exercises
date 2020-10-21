import pandas as pd
import numpy as np
from env import user, password, host
from sklearn.model_selection import train_test_split


def handle_missing_values(df, prop_required_column = .60, prop_required_row = .60):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df

def prep_zillow_data(df):
    df = df[df.propertylandusetypeid.isin([260, 261, 262, 279])]
    df = df[(df.bedroomcnt > 0) & (df.bathroomcnt > 0)]
    df = df.drop(columns=["propertylandusetypeid", "heatingorsystemtypeid", 'propertyzoningdesc', 'calculatedbathnbr', 'parcelid', 'id'])
    df = handle_missing_values(df)
    df.heatingorsystemdesc = df.heatingorsystemdesc.fillna('None')
    train_and_validate, test = train_test_split(df, test_size=.2, random_state=123)
    train, validate = train_test_split(train_and_validate, test_size=.3, random_state=123)
    cols = [
    'buildingqualitytypeid',
    'regionidcity',
    'regionidzip',
    'yearbuilt',
    'censustractandblock'
    ]
    for col in cols:
        mode = int(train[col].mode())
        train[col].fillna(value=mode, inplace=True)
        validate[col].fillna(value=mode, inplace=True)
        test[col].fillna(value=mode, inplace=True)
    cols = [
    'structuretaxvaluedollarcnt',
    'taxamount',
    'taxvaluedollarcnt',
    'landtaxvaluedollarcnt',
    'structuretaxvaluedollarcnt',
    'finishedsquarefeet12',
    'calculatedfinishedsquarefeet',
    'fullbathcnt',
    'lotsizesquarefeet'
    ]
    for col in cols:
        median = train[col].median()
        train[col].fillna(value=median, inplace=True)
        validate[col].fillna(value=median, inplace=True)
        test[col].fillna(value=median, inplace=True)
    train.to_csv('zillow_train.csv')
    validate.to_csv('zillow_validate.csv')
    test.to_csv('zillow_test.csv')
    return train, validate, test