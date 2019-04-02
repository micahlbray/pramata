# -*- coding: utf-8 -*-

from math import ceil
import csv
import sys
import pandas as pd
import pyodbc
from threading import Thread
from datetime import datetime

main_path = r"\\NAX" 
sys.path.append(main_path)
from NAXGeocoding import NAXGeocoding

#clientId = ""
#clientSecret = "" 
#url = r""
#webSecUrl = r""
#geocode_service = NAXGeocoding(url, webSecUrl, clientId, clientSecret)

def CallNax(geocode_service, data_input):
    N = data_input.shape[0]
    data_input['pramata_number'] = data_input['pramata_number'].fillna(0.0).astype(int)
    data_input['Address_Number'] = data_input['Address_Number'].fillna(0.0).astype(int)
    out_list = []
    for i in range(N):
        entry = data_input.iloc[i,:]
    
        for j in range(entry.shape[0]):
            if entry[j] is None:
                entry[j] = ''
        pramata_number = str(entry['pramata_number'])
        address_number = str(entry['Address_Number'])
        addr = entry['Address_Of_Leased_Space_Address_1']
        addr2 = ''
        city = entry['Address_Of_Leased_Space_City']
        state = entry['Address_Of_Leased_Space_State_Province']
        zip5 = entry['Address_Of_Leased_Space_Zip_Postal_Code']
    
        if len(state) > 2:
            if len(zip5) == 2:
                temp_val = state
                state = zip5
                try:
                    zip5 = int(temp_val)
                    zip5 = str(zip5)
                except: 
                    zip5 = ''
            else:
                state = ''
        if len(zip5) != 5:
            zip5 = ''
        else:
            try:
                int(zip5)
            except:
                zip5 = ''

        response = geocode_service.find_address(addr, addr2, city, state, zip5)
        
        #flag thrown for unsuccessful match
        if response['statusText'] == '[0] Addresses found.':
            continue
    
        stnd_addr = response['standardizedAddress']
        addr_info = response['addresses'][0] 
    
        addr = stnd_addr['addressLine1']
        city = addr_info['city']
        state = stnd_addr['state']
        zip5 = stnd_addr['zip5']
        try:
            lat = str(stnd_addr['latitude'])
            long = str(stnd_addr['longitude'])
            code = stnd_addr['geoResultCode']
        except:
            lat = ''
            long = ''
            code = ''
            
        addr2 = ''
        exactmatch = 'N'
        eloc_val = '0'
        eps_val = '0'
        if len(addr_info['addressUnits']) == 0:
            unit = addr_info
            if unit['exactMatchYn'] == 'Y':
                try:
                    addr2 = unit['unitValue']
                except:
                    addr2 = ''
                nax_val = str(unit['addressGUID'])
                exactmatch = 'Y'
                for origin in unit['addressOrigins']:
                    if origin['origin'] == 'epsilon':
                        eps_val = origin['originKey']
                    elif origin['origin'] in ['eloc', 'eloc-alternate']:
                        eloc_val = origin['originKey']
                addr = addr.replace("'","''")
                city = city.replace("'","''")
                output = (pramata_number, address_number, exactmatch, 
                          addr, addr2, city, state, zip5, 
                          lat, long, code, eps_val, nax_val, eloc_val)
                out_list.append(output)  
            else:
                addr = addr.replace("'","''")
                city = city.replace("'","''")
                for unit in addr_info['addressUnits']:
                    eps_val = '0'
                    eloc_val = '0'
                    addr2 = unit['unitValue']
                    nax_val = str(unit['addressGUID'])
                    for origin in unit['addressOrigins']:
                        if origin['origin'] == 'epsilon':
                            eps_val = origin['originKey']
                        elif origin['origin'] in ['eloc', 'eloc-alternate']:
                            eloc_val = origin['originKey']
                
                    output = (pramata_number, address_number, exactmatch, 
                              addr, addr2, city, state, zip5, 
                              lat, long, code, eps_val, nax_val, eloc_val)
                    out_list.append(output) 
            continue #go to next address
                
            
        for unit in addr_info['addressUnits']:
            if unit['exactMatchYn'] == 'Y':
                addr2 = unit['unitValue']
                nax_val = str(unit['addressGUID'])
                exactmatch = 'Y'
                for origin in unit['addressOrigins']:
                    if origin['origin'] == 'epsilon':
                        eps_val = origin['originKey']
                    elif origin['origin'] in ['eloc', 'eloc-alternate']:
                        eloc_val = origin['originKey']
                addr = addr.replace("'","''")
                city = city.replace("'","''")
                output = (pramata_number, address_number, exactmatch, 
                          addr, addr2, city, state, zip5, 
                          lat, long, code, eps_val, nax_val, eloc_val)
                out_list.append(output)        
                break

        if exactmatch == 'N': 
            addr = addr.replace("'","''")
            city = city.replace("'","''")
            for unit in addr_info['addressUnits']:
                eps_val = '0'
                eloc_val = '0'
                addr2 = unit['unitValue']
                nax_val = str(unit['addressGUID'])
                for origin in unit['addressOrigins']:
                    if origin['origin'] == 'epsilon':
                        eps_val = origin['originKey']
                    elif origin['origin'] in ['eloc', 'eloc-alternate']:
                        eloc_val = origin['originKey']
                
                output = (pramata_number, address_number, exactmatch, 
                          addr, addr2, city, state, zip5, 
                          lat, long, code, eps_val, nax_val, eloc_val)
                out_list.append(output) 
    return out_list

def DealEntries(df_input,n_sets):
    n_rows = df_input.shape[0]
    entry_per_set = n_rows // n_sets
    extra = n_rows % n_sets
    
    outlist = []
    for i in range(n_sets):
        if i != n_sets - 1:
            idx = range(0+entry_per_set * i, entry_per_set * (i + 1))
        else:
            idx = range(0+entry_per_set * i, entry_per_set * (i + 1) + extra)
        outlist.append(idx)
    return outlist

class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)

        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self):
        Thread.join(self)
        return self._return
    
def AddressCleanup():     
    # NAX Client Connection Information
    clientId = ""
    clientSecret = "" 
    url = r""
    webSecUrl = r""
        
    # SQL connection information
    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};'
                      'SERVER=;DATABASE=BI_MIP;'
                      'Trusted_Connection=yes')
    cursor = cnxn.cursor()
    
    output_table = 'BI_MIP.MIP2.PRAMATA_NUMBER_ADDRESS_OUTPUT'
    output_colums = ('pramata_number','Address_Number','EXACT_MATCH',
                         'BUILDING_ADDRESS','MATCHED_ADDRESS_2','BUILDING_CITY',
                         'BUILDING_STATE','BUILDING_ZIP','BUILDING_LAT',
                         'BUILDING_LONG', 'BUILDING_CODE', 'EPS_ADDRESS_ID',
                         'NAX_ADDRESS_ID', 'ELOC_ID')
    
    # Load information from SQL
    sql = (''' EXEC BI_MIP.MIP2.pramata_number_nax_AddressCleanup_df ''')
    #sql = (''' SELECT	DISTINCT a.*
    #            FROM	mip2.PRAMATA_NUMBER_ADDRESS as a
    #            WHERE	a.IsActive = 1
    #            		AND pramata_number = '183056'
    #      ''')
    data_input = pd.read_sql(sql, cnxn)

    # Create job threads, and iterate over each
    threads = 20
    rows_per_thread = 1000
    n_rows = data_input.shape[0]
    blocks = ceil(n_rows/(threads*rows_per_thread))
    blocklist = DealEntries(data_input, blocks)
    completed = 0
    
    data_to_write = []
    print("Beginning Address Cleanup")
    for block_idx in range(blocks):
        start = datetime.now()
        data_block = data_input.iloc[blocklist[block_idx],:][:]
        outlist = DealEntries(data_block, threads)
        jobs = []
        #data_to_write = []
        for i in range(threads):
            curr_input = data_block.iloc[outlist[i],:]
            geocode_service = NAXGeocoding(url, webSecUrl, clientId, clientSecret)
            thread = ThreadWithReturnValue(target=CallNax, args=(geocode_service, curr_input[:]))
            jobs.append(thread)
    
        for j in jobs:
            j.start()
        
        for j in jobs:
            try:
                data_to_write = data_to_write + j.join()
            except:
                continue
            
        timediff = datetime.now() - start
        print('{} Threads completed in {}'.format(len(blocklist[block_idx]), 
                                                  timediff))
            
        for output in data_to_write:
            value_str = "('" + "','".join(output) + "')"
            sql = ('INSERT INTO {} VALUES {}'.format(output_table, value_str))
            cursor.execute(sql)
            cursor.commit()
        #cursor.commit()
        #cnxn.close()
        
        completed += len(blocklist[block_idx])
        print("SQL Processing complete for {} out of {} entries"
                     .format(completed, data_input.shape[0]))

    
if __name__ == "__main__":
    AddressCleanup()

