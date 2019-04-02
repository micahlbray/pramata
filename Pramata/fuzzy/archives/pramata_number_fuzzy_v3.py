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

def pramata_number_fuzzy():
    # Import data
    sql =   ('''   select *
                   from bi_mip.mip2.PRAMATA_NUMBER_FUZZ 
                   where pramata_owner_name is not null 
            ''')
    df = pd.read_sql(sql, cnxn)

    df['Token_Set_Ratio'] = 0
    df['Token_Set_Name'] = ''

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


    # Write file to excel
    #df.to_excel(r'C:\Desktop\Pramata_Number_Fuzzy_Output.xlsx')
    df.to_sql(name='PRAMATA_NUMBER_FUZZ_OUTPUT',con=cnxn,schema='MIP2',
                            if_exists='replace',index=False)
