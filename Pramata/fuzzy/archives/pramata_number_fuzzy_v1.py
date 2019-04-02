# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import time
from fuzzywuzzy import fuzz
from tqdm import tqdm
import sqlalchemy
from urllib import parse

cnxn = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'
                .format(parse.quote_plus('DRIVER={SQL Server Native Client 11.0};'
                                    'SERVER=;DATABASE=BI_MIP;'
                                    'Trusted_Connection=yes;')
                        )
                )

# Time logging
now = datetime.datetime.now()
print('Script Started')
print(now)
start_time = time.time()

# Import data
sql =   ('''   select *
               from bi_mip.mip2.PRAMATA_NUMBER_FUZZ 
               where pramata_owner_name is not null 
        ''')
df = pd.read_sql(sql, cnxn)

#df['Simple_Ratio'] = 0
#df['Simple_Name'] = ''
#df['Partial_Ratio'] = 0
#df['Partial_Name'] = ''
#df['Token_Sort_Ratio'] = 0
#df['Token_Sort_Name'] = ''
df['Token_Set_Ratio'] = 0
df['Token_Set_Name'] = ''

# Time logging
print('\nAppending Simple Ratios...')
print('%s seconds' % (time.time()- start_time))

# Iterate through nationals file and tag majors file with simple ratio
for index, row in tqdm(df.iterrows()):
    pram_name = str(row['Pramata_Owner_Name'])
    costar_name = str(row['Costar_Owner_Name'])
    ratio = 0
    best_name = ''
    curr_ratio = fuzz.ratio(pram_name, costar_name)
    if curr_ratio > ratio:
        ratio = curr_ratio
        best_name = costar_name
        
    df.loc[index, 'Simple_Ratio'] = ratio
    df.loc[index, 'Simple_Name'] = best_name
       
# Time logging
print('\nAppending Partial Ratios...')
print('%s seconds' % (time.time()- start_time))

# Iterate through nationals file and tag majors file with partial ratio
for index, row in tqdm(df.iterrows()):
    pram_name = str(row['Pramata_Owner_Name'])
    costar_name = str(row['Costar_Owner_Name'])
    ratio = 0
    best_name = ''
    curr_ratio = fuzz.partial_ratio(pram_name, costar_name)
    if curr_ratio > ratio:
        ratio = curr_ratio
        best_name = costar_name
        
    df.loc[index, 'Partial_Ratio'] = ratio
    df.loc[index, 'Partial_Name'] = best_name

# Time logging
print('\nAppending Token Sort Ratios...')
print('%s seconds' % (time.time()- start_time))
        
# Iterate through nationals file and tag majors file with token sort ratio
for index, row in tqdm(df.iterrows()):
    pram_name = str(row['Pramata_Owner_Name'])
    costar_name = str(row['Costar_Owner_Name'])
    ratio = 0
    best_name = ''
    curr_ratio = fuzz.token_sort_ratio(pram_name, costar_name)
    if curr_ratio > ratio:
        ratio = curr_ratio
        best_name = costar_name
        
    df.loc[index, 'Token_Sort_Ratio'] = ratio
    df.loc[index, 'Token_Sort_Name'] = best_name

# Time logging
print('\nAppending Token Set Ratios...')
print('%s seconds' % (time.time()- start_time))

# Iterate through nationals file and tag majors file with token set ratio
for index, row in tqdm(df.iterrows()):
    pram_name = str(row['Pramata_Owner_Name'])
    costar_name = str(row['Costar_Owner_Name'])
    ratio = 0
    best_name = ''
    curr_ratio = fuzz.token_set_ratio(pram_name, costar_name)
    if curr_ratio > ratio:
        ratio = curr_ratio
        best_name = costar_name
        
    df.loc[index, 'Token_Set_Ratio'] = ratio
    df.loc[index, 'Token_Set_Name'] = best_name  

# Time logging
print('\nWriting To BI_MIP...')
print('%s seconds' % (time.time()- start_time))

# Write file to excel
#df.to_excel(r'C:\Users\mbray201\Desktop\Pramata_Number_Fuzzy_Output.xlsx')
df.to_sql(name='PRAMATA_NUMBER_FUZZ_OUTPUT',con=cnxn,schema='MIP2',
                        if_exists='append',index=False)
    
# Time logging
now = datetime.datetime.now()
print('\nScript Complete')
print(now)
