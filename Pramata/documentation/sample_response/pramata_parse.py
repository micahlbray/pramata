# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 14:44:59 2018

@author: mbray201
"""

import json
import pandas as pd
import pyodbc
import sqlalchemy, urllib

pramata_number_file = 'C:/Users/mbray201/Desktop/market_intelligence/API/Pramata/sample_response/pramata_number.txt'
keyterms_file = 'C:/Users/mbray201/Desktop/market_intelligence/API/Pramata/sample_response/keyterms.txt'
keydates_file = 'C:/Users/mbray201/Desktop/market_intelligence/API/Pramata/sample_response/keydates.txt'

def pramata_number_parse():
    with open(pramata_number_file) as data_file:    
        pramata_number_json = json.load(data_file)
    
    test = {}
    metadata = pramata_number_json['metadata']
    for key, value in metadata.items():
        test[key] = value
    
    keys_list = ['Parcel_Number','Courtesy_Services','Referral_Partner'
            ,'National_Account','Mdu','Owner_Occupied','Fee','Notes'
            ,'Ae_Document_Title_Internal','Ae_Document_Type_Internal'
            ,'Ae_Effective_Date_Internal','Ae_Company_Group_Internal'
            ,'Ae_Contract_Model_Internal','Region_Imported','Building_Id','Validator'
            ,'Initial_Validation','Validation_Resolution','Area']
    keyterms = pramata_number_json['keyterms']['keyterm']
    for i in range(len(keyterms)):
        group_name = keyterms[i]['api_name']
        x = keyterms[i]['terms'][0]['dataelements']
        for n in range(len(x)):
            if group_name not in keys_list:
                subgroup_name = group_name + "_" + x[n]['api_name']
            else:
                subgroup_name = x[n]['api_name']
            val = x[n]['data']
            test[subgroup_name] = val ### made val a list because of error creating df
    
    result = {}
    clean = []
    for key, value in test.items():
        if key.lower() not in clean:
            result[key] = value
            clean.append(key.lower())
    
    result = [result]
    df = pd.DataFrame(result)
    #df = pd.DataFrame.from_dict(result, orient='columns') 
    cnxn = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'
                .format(urllib.parse.quote_plus('DRIVER={SQL Server Native Client 11.0};'
                                    'SERVER=WestBIS-RPT;DATABASE=BI_MIP;'
                                    'Trusted_Connection=yes;')
                        )
                )
    df.to_sql(name='PRAMATA_DETAIL', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)

##################################################################
# Keydates file 
##################################################################
def pramata_keydates_parse():
    with open(keydates_file) as data_file:    
        keydates_json = json.load(data_file)
        #print(data)
    
    pramata_numbers = []
    status = []
    numbers = keydates_json['pramataNumbers']
    for i in range(len(numbers)):
        pramata_number = numbers[i]['pramata_number']
        is_deleted = numbers[i]['is_deleted']
        pramata_numbers.append(pramata_number)
        status.append(is_deleted)
    
    
    test = {'pramata_number': pramata_numbers, 'status': status}
    df = pd.DataFrame(test)
    print(df)
    
    date_range = keydates_json['date_range']
    start_date = date_range[0]['start_date_timestamp']
    end_date = date_range[0]['end_date_timestamp']

    len(df.index) 

##################################################################
# Key Terms file 
##################################################################
def pramata_keyterms_parse():
    with open(keyterms_file) as data_file:    
        keyterms_json = json.load(data_file)
        #print(keyterms_json)
    badvalues = ['Parcel_Number','Courtesy_Services','Referral_Partner'
            ,'National_Account','Mdu','Owner_Occupied','Fee','Notes'
            ,'Ae_Document_Title_Internal','Ae_Document_Type_Internal'
            ,'Ae_Effective_Date_Internal','Ae_Company_Group_Internal'
            ,'Ae_Contract_Model_Internal','Region_Imported','Building_Id','Validator'
            ,'Initial_Validation','Validation_Resolution','Area']
    test = {}
    asdf = []
    x = keyterms_json['details'][0]['keyterms']
    for i in range(len(x)):
        group_name = x[i]['api-name']
        y = x[i]['dataelements']
        subtest = []
        dt = {}
        if group_name not in badvalues:
            for n in range(len(y)):
                subgroup_name = y[n]['api-name']
                asdf_name = group_name + "_" + y[n]['api-name']
                data_type = y[n]['data-type']
                subtest.append(subgroup_name)
                asdf.append(asdf_name)
                dt[asdf_name] = data_type
            #test[group_name] = dt
            print(dt)
        else:
            test[group_name] = y['data-type']
            asdf.append(group_name)
  
pramata_number_parse()