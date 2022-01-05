# FUNCTIONS FOR WRANGLING ZILLOW DATA

import pandas as pd

def get_url(db):
    '''
    This function takes in a database name and returns a url (using the specified 
    database name as well as host, user, and password from env.py) for use in the 
    pandas.read_sql() function.
    '''
    from env import host, user, password
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def acquire_zillow():
    '''
    This function pulls data from the SQL zillow database and caches that data to a csv
    for later data retrieval. It takes no arguments and returns a dataframe of zillow data.
    '''
    import os
    if os.path.isfile('zillow.csv'):
        zillow = pd.read_csv('zillow.csv', index_col=0)
        return zillow
    else:        
        sql = '''
            SELECT bathroomcnt as baths, bedroomcnt as beds, calculatedfinishedsquarefeet as sq_ft, fips, fullbathcnt as fullbaths, latitude,
                   longitude, roomcnt as rooms, yearbuilt, taxvaluedollarcnt as tax_value, garagecarcnt, logerror, transactiondate, 
                   unitcnt, propertylandusetypeid
            FROM properties_2017
            LEFT JOIN predictions_2017 pred USING(parcelid)
            LEFT JOIN airconditioningtype USING(airconditioningtypeid)
            LEFT JOIN architecturalstyletype USING(architecturalstyletypeid)
            LEFT JOIN buildingclasstype USING(buildingclasstypeid)
            LEFT JOIN heatingorsystemtype USING(heatingorsystemtypeid)
            LEFT JOIN propertylandusetype USING(propertylandusetypeid)
            LEFT JOIN storytype USING(storytypeid)
            LEFT JOIN typeconstructiontype USING(typeconstructiontypeid)
            WHERE latitude IS NOT NULL
            AND longitude IS NOT NULL
            AND transactiondate LIKE "2017%%"
            AND pred.id IN (SELECT MAX(id)
            FROM predictions_2017
            GROUP BY parcelid
            HAVING MAX(transactiondate));
            '''
        zillow = pd.read_sql(sql, get_url('zillow'))
        zillow.to_csv('zillow.csv')
        return zillow

def cols_missing_rows(df):
    '''
    This function takes in a dataframe and returns a dataframe of column names, the number
    of rows that column is missing, and the percentage of rows that column is missing.
    '''
    df = pd.DataFrame(data={'num_rows_missing':df.isnull().sum(), 
              'pct_rows_missing':df.isnull().sum()/len(df)}, index=df.columns)
    return df

def rows_missing_cols(df):
    '''
    This function takes in a dataframe and returns a dataframe of the number of columns
    missing from a row, the percentage of columns missing from a row, and the number of
    rows that are missing that number/percentage of columns.
    '''
    df = pd.DataFrame({'num_cols_missing':df.isnull().sum(axis=1).value_counts().index,
                       'pct_cols_missing':df.isnull().sum(axis=1).value_counts().index/len(df.columns),
                       'num_rows':df.isnull().sum(axis=1).value_counts()}).reset_index(drop=True)
    return df

def only_single_units(zillow):
    '''
    This function takes in the zillow dataframe and removes any properties not believed
    to be single-unit properties. It returns zillow without those properties.
    '''
    zillow_filt = zillow[zillow.propertylandusetypeid.isin([261, 262, 263, 264, 266, 268, 273, 276, 279])]
    zillow_filt = zillow_filt[(zillow.baths > 0) & (zillow.sq_ft > 300)]
    zillow_filt = zillow_filt[(zillow_filt.unitcnt == 1) | (zillow_filt.unitcnt.isnull())]
    return zillow_filt

def handle_missing_values(df, prop_req_col, prop_req_row):
    '''
    This function takes in a dataframe, a max proportion of null values for each 
    column, and a max proportion of null values for each row. It returns the 
    dataframe less any rows or columns with more than the max proportion of nulls.
    '''
    df = df.dropna(axis=1, thresh=prop_req_col*len(df))
    df = df.dropna(thresh=prop_req_row*len(df.columns))
    return df

def label_fips(zillow):
    zillow['fips'] = zillow.fips.astype(int)
    zillow['fips_loc'] = zillow.fips.replace({6037:'Los Angeles, CA',
                       6059:'Orange, CA',
                       6111:'Ventura, CA'})
    return zillow

def wrangle_zillow(prop_req_col, prop_req_row):
    '''
    This function wrangles zillow data. It takes in thresholds for null values which are
    used to drop columns and rows with too many nulls. The function returns a dataframe.
    '''
    zillow = handle_missing_values(only_single_units(acquire_zillow()), prop_req_col, prop_req_row)
    zillow = zillow.drop(columns=['unitcnt', 'propertylandusetypeid']).dropna()
    zillow = label_fips(zillow)
    return zillow