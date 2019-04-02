# -*- coding: utf-8 -*-
"""
Created on Wed Apr  4 18:07:14 2018

@author: mbray201
"""

import pyodbc
import pandas as pd
import time
from pramata_parse import pramata_keydates_parse, pramata_number_parse
from pramata_requests import (pramata_key_dates_req, 
                              pramata_number_req,
                              getOAuthToken,
                              )

cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};'
                      'SERVER=;DATABASE=BI_MIP;'
                      'Trusted_Connection=yes')

##### test sites credentials
#clientId = ''
#clientSecret = ''
#base_url = ''
##### prod site credentials
clientId = ''
clientSecret = ''
base_url = ''
oauth_url = '/auth/oauth/v2/token'
pramata_number_url = '/services/data/v1/documents/{pramata_number}/details'
keydates_url = '/services/data/v1/documents/modified?start_date_timestamp={start_date_timestamp}&end_date_timestamp={end_date_timestamp}'
keyterms_url = ''

def keydates_load(token, start_date, end_date, i):
    try:
        response = pramata_key_dates_req(token, start_date, end_date)
    except:
        token = getOAuthToken(base_url, oauth_url, clientId, clientSecret)
        response = pramata_key_dates_req(token, start_date, end_date)
    
    code = 000 ## clean code
    message = 'All good.'
    
    try: 
        try: ### Handle 200 code error
            code = response['response_details']['code']
            message = response['response_details']['message']
        except: ### Handle 500 code error (system unavailable)
            code = response['error']['code']
            message = response['error']['message']
    except:
        code = code
        message = message
    
    results = {}
    if code == 000:
        pramata_keydates_parse(response)
        result = {}
        result['AttemptedOn'] = time.strftime("%Y-%m-%d %H:%M")
        result['start_date'] = start_date
        result['end_date'] = end_date
        result['code'] = code
        result['message'] = message 
        results[i] = result
    else:
        result = {}
        result['AttemptedOn'] = time.strftime("%Y-%m-%d %H:%M")
        result['start_date'] = start_date
        result['end_date'] = end_date
        result['code'] = code
        result['message'] = message 
        results[i] = result
    print(results)
    return results

def pramata_number_load(token, pramata_number, i):
    print(i)
    print(pramata_number)
    print("\n")
    try:
        response = pramata_number_req(token, pramata_number)
    except:
        token = getOAuthToken(base_url, oauth_url, clientId, clientSecret)
        response = pramata_number_req(token, pramata_number)
    code = 000 ## clean code
    try: 
        try: ### Handle 200 code error
            code = response['response_details']['code']
            message = response['response_details']['message']
        except: ### Handle 500 code error (system unavailable)
            code = response['error']['code']
            message = response['error']['message']
    except:
        code = code
        message = 'All good.'
    
    results = {}
    if code == 000:
        groups = pramata_number_parse(response, pramata_number)
    else:
        result = {}
        result['AttemptedOn'] = time.strftime("%Y-%m-%d %H:%M")
        result['pramata_number'] = pramata_number
        result['code'] = code
        result['message'] = message 
        results[i] = result
        groups = {}
    return groups
        
######################################################################
######################################################################
def keydates_load_loop():
    sql = (''' EXEC BI_MIP.MIP2.pramata_number_keydates_df ''')
    df = pd.read_sql(sql, cnxn)
    
    token = getOAuthToken(base_url, oauth_url, clientId, clientSecret)
    for i in range(len(df.index)):
        start_date = str(df['start_date'].values[i])[:10]
        end_date = str(df['end_date'].values[i])[:10]
        print('Between ' + start_date + ' and ' + end_date)
        keydates_load(token, start_date, end_date, i)

######################################################################
######################################################################
def pramata_number_load_loop():
    sql = (''' EXEC BI_MIP.MIP2.pramata_number_df ''')
    df = pd.read_sql(sql, cnxn)
    
    result = []
    token = getOAuthToken(base_url, oauth_url, clientId, clientSecret)
    for i in range(len(df.index)):
        pramata_number = str(df['pramata_number'].values[i])
        result.append(pramata_number_load(token, pramata_number, i))

    file = open('test.txt','w')
    for item in result:
        file.write("%s\n" % item)

def LoadData():
    keydates_load_loop()
    pramata_number_load_loop()
