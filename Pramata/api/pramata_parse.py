# -*- coding: utf-8 -*-
"""
Created on Tue Apr  3 14:44:59 2018

@author: mbray201
"""

import time
import json
import pandas as pd
import pyodbc
import sqlalchemy
from urllib import parse

cnxn = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'
                .format(parse.quote_plus('DRIVER={SQL Server Native Client 11.0};'
                                    'SERVER=;DATABASE=BI_MIP;'
                                    'Trusted_Connection=yes;')
                        )
                )

def pramata_number_parse(response, pramata_number):
    data = {}
    addresses = []
    terms = []
    signs = []
    contacts = []
    agreements = []
    renewals = []
    signatures = []
    contractstats = []
    groups = {}
    metadata = response['metadata']
    for key, value in metadata.items():
        data[key] = value
    
    keys_list = ['Parcel_Number','Courtesy_Services','Referral_Partner'
            ,'National_Account','Mdu','Owner_Occupied','Fee','Notes'
            ,'Ae_Document_Title_Internal','Ae_Document_Type_Internal'
            ,'Ae_Effective_Date_Internal','Ae_Company_Group_Internal'
            ,'Ae_Contract_Model_Internal','Region_Imported','Building_Id'
            ,'Validator','Initial_Validation','Validation_Resolution','Area'
            ,'Contract_Status_1','Renewal_Start_Date_Internal','Suite_Specific'
            ,'Fees','Courtesy_Service','Exception_Document','Date_Of_Transfer'
            ,'Date_Of_Release','Mall_Building_Name'
            ,'Number_Of_Remaining_Renewals','Engagement_Id_Imported']
    keyterms = response['keyterms']['keyterm']
    for i in range(len(keyterms)):
        group_name = keyterms[i]['api_name']
        x = keyterms[i]['terms']
        y = keyterms[i]['terms'][0]['dataelements']
        if group_name not in keys_list:
            for p in range(len(x)):
                z = x[p]['dataelements']
                address = {}
                term = {}
                sign = {}
                contact = {}
                agreement = {}
                renewal = {}
                signature = {}
                contractstat = {}
                if group_name == 'Address_Of_Leased_Space':
                    for n in range(len(z)):
                        subgroup_name = group_name + "_" + z[n]['api_name']                        
                        val = z[n]['data']
                        address[subgroup_name] = val
                    addresses.append(address)
                elif group_name == 'Term_Renewal_And_Expiration_Dates':
                    for n in range(len(z)):
                        subgroup_name = group_name + "_" + z[n]['api_name']                        
                        val = z[n]['data']
                        term[subgroup_name] = val
                    terms.append(term)
                elif group_name == 'Customer_Signatory':
                    for n in range(len(z)):
                        subgroup_name = group_name + "_" + z[n]['api_name']                        
                        val = z[n]['data']
                        sign[subgroup_name] = val
                    signs.append(sign)
                elif group_name == 'Site_Contact':
                    for n in range(len(z)):
                        subgroup_name = group_name + "_" + z[n]['api_name']                        
                        val = z[n]['data']
                        contact[subgroup_name] = val
                    contacts.append(contact)
                elif group_name == 'Agreement_Term_Renewal_And_Expiration':
                    for n in range(len(z)):
                        subgroup_name = group_name + "_" + z[n]['api_name']                        
                        val = z[n]['data']
                        agreement[subgroup_name] = val
                    agreements.append(agreement)
                elif group_name == 'Renewal_Term_Months_Internal':
                    for n in range(len(z)):
                        subgroup_name = group_name + "_" + z[n]['api_name']                        
                        val = z[n]['data']
                        renewal[subgroup_name] = val
                    renewals.append(renewal)
                elif group_name == 'Signature_Status':
                    for n in range(len(z)):
                        subgroup_name = group_name + "_" + z[n]['api_name']                        
                        val = z[n]['data']
                        signature[subgroup_name] = val
                    signatures.append(signature)
                elif group_name == 'Contract_Status':
                    for n in range(len(z)):
                        subgroup_name = group_name + "_" + z[n]['api_name']                        
                        val = z[n]['data']
                        contractstat[subgroup_name] = val
                    contractstats.append(contractstat)
                else:
                    groups[group_name] = len(x)
        else:
            for n in range(len(y)):
                if group_name in keys_list:
                    subgroup_name = y[n]['api_name']
                    val = y[n]['data']
                    data[subgroup_name] = val
                else:
                    pass
                    #subgroup_name = group_name + "_" + y[n]['api_name']
                
                #val = y[n]['data']
                #data[subgroup_name] = val

    ## clean up potential duplicate data from metadata to keyterms
    result = {}
    clean = []
    for key, value in data.items():
        if key.lower() not in clean:
            result[key] = value
            clean.append(key.lower())
    
    ## create dataframe for detail and write to DB
    result = [result]
    df = pd.DataFrame(result)
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_DETAIL', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)
    
    ## create dataframe for addresses and write to DB
    df = pd.DataFrame(addresses)
    pramta_numbers = [pramata_number for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['pramata_number'] = pramta_numbers
    df['address_number'] = [str(n)for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_ADDRESS', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)
    
    ## create dataframe for terms and write to DB
    df = pd.DataFrame(terms)
    pramta_numbers = [pramata_number for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['pramata_number'] = pramta_numbers
    df['term_number'] = [str(n)for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_TERM', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)
    
    ## create dataframe for signatories and write to DB
    df = pd.DataFrame(signs)
    pramta_numbers = [pramata_number for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['pramata_number'] = pramta_numbers
    df['signatory_number'] = [str(n)for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_SIGNATORY', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)
    
    ## create dataframe for contacts and write to DB
    df = pd.DataFrame(contacts)
    pramta_numbers = [pramata_number for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['pramata_number'] = pramta_numbers
    df['contact_number'] = [str(n)for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_CONTACT', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)
    
    ## create dataframe for agreements and write to DB
    df = pd.DataFrame(agreements)
    pramta_numbers = [pramata_number for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['pramata_number'] = pramta_numbers
    df['agreement_term_number'] = [str(n)for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_AGREEMENT_TERM', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)
    
    ## create dataframe for renewals and write to DB
    df = pd.DataFrame(renewals)
    pramta_numbers = [pramata_number for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['pramata_number'] = pramta_numbers
    df['renewal_term_number'] = [str(n)for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_RENEWAL_TERM', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)
    
    ## create dataframe for signatures and write to DB
    df = pd.DataFrame(signatures)
    pramta_numbers = [pramata_number for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['pramata_number'] = pramta_numbers
    df['signature_status_number'] = [str(n)for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_SIGNATURE_STATUS', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)

    ## create dataframe for contract status and write to DB
    df = pd.DataFrame(contractstats)
    pramta_numbers = [pramata_number for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    loaded_by = ['Pramata API' for n in range(len(df.index))]
    df['pramata_number'] = pramta_numbers
    df['contract_status_number'] = [str(n)for n in range(len(df.index))]
    df['loadedon'] = loaded_dates
    df['loadedby'] = loaded_by
    df.to_sql(name='PRAMATA_NUMBER_CONTRACT_STATUS', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)
    
    return groups

##################################################################
# Keydates file 
##################################################################
def pramata_keydates_parse(response):  
    pramata_numbers = []
    status = []
    pramataNumbers = response['pramataNumbers']
    for i in range(len(pramataNumbers)):
        pramata_number = pramataNumbers[i]['pramata_number']
        is_deleted = pramataNumbers[i]['is_deleted']
        pramata_numbers.append(pramata_number)
        status.append(is_deleted)
    
    date_range = response['date_range']
    start_date = date_range[0]['start_date_timestamp']
    end_date = date_range[0]['end_date_timestamp']
    
    data = {'pramata_number': pramata_numbers, 'is_deleted': status}
    df = pd.DataFrame(data)
    start_dates = [start_date for n in range(len(df.index))]
    end_dates = [end_date for n in range(len(df.index))]
    loaded_dates = [time.strftime("%Y-%m-%d %H:%M") for n in range(len(df.index))]
    
    
    df['start_date_timestamp'] = start_dates
    df['end_date_timestamp'] = end_dates
    df['loadedon'] = loaded_dates

    ## create dataframe and write to DB
    df.to_sql(name='PRAMATA_NUMBER', con=cnxn,
                     schema='MIP2', if_exists='append', index=False)

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
            ,'Ae_Contract_Model_Internal','Region_Imported','Building_Id'
            ,'Validator','Initial_Validation','Validation_Resolution','Area'
            ,'Contract_Status_1','Renewal_Start_Date_Internal','Suite_Specific'
            ,'Fees','Courtesy_Service','Exception_Document','Date_Of_Transfer'
            ,'Date_Of_Release','Mall_Building_Name'
            ,'Number_Of_Remaining_Renewals','Engagement_Id_Imported']
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


#pramata_keyterms_parse()         
  
