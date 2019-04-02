# -*- coding: utf-8 -*-
"""
Created on Wed Jul 18 12:57:43 2018

@author: mbray201
"""

import pyodbc
import sys

main_path = r"\\\Pramata" 
sys.path.append(main_path)

import datetime

from api import pramata_load as api
from nax import pramata_call_nax as nax
from fuzzy import pramata_number_fuzzy as fuzzy

cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};'
                      'SERVER=;DATABASE=BI_MIP;'
                      'Trusted_Connection=yes')
cursor = cnxn.cursor()


#api.LoadData()

currentDT = datetime.datetime.now()
print("Starting pramata_number_address_update at " + str(currentDT))
sql = (''' EXEC MIP2.pramata_number_address_update ''')
cursor.execute(sql)
cursor.commit()

currentDT = datetime.datetime.now()
print("\nStarting AddressCleanup at " + str(currentDT))
nax.AddressCleanup()

currentDT = datetime.datetime.now()
print("\nStarting pramata_number_fuzz_truncate_insert at " + str(currentDT))
sql = (''' EXEC BI_MIP.MIP2.pramata_number_fuzz_truncate_insert ''')
cursor.execute(sql)
cursor.commit()

currentDT = datetime.datetime.now()
print("\nStarting FuzzyMatch at " + str(currentDT))
fuzzy.FuzzyMatch()

currentDT = datetime.datetime.now()
print("\nStarting pramata_sfdc_truncate_insert at " + str(currentDT))
sql = (''' EXEC MIP2.pramata_sfdc_truncate_insert ''')
cursor.execute(sql)
cursor.commit()

currentDT = datetime.datetime.now()
print("\nStarting pramata_xref_roe_engagement_location_truncate_insert at " + str(currentDT))
sql = (''' EXEC MIP2.pramata_xref_roe_engagement_location_truncate_insert ''')
cursor.execute(sql)
cursor.commit()

currentDT = datetime.datetime.now()
print("\nStarting pramata_xref_detail_truncate_insert at " + str(currentDT))
sql = (''' EXEC BI_MIP.MIP2.pramata_xref_detail_truncate_insert ''')
cursor.execute(sql)
cursor.commit()

currentDT = datetime.datetime.now()
print("\nStarting pramata_xref_roe_building_truncate_insert at " + str(currentDT))
sql = (''' EXEC MIP2.pramata_xref_roe_building_truncate_insert ''')
cursor.execute(sql)
cursor.commit()

currentDT = datetime.datetime.now()
print("\nStarting pramata_roe_gate_drop_create at " + str(currentDT))
sql = (''' EXEC MIP2.pramata_roe_gate_drop_create ''')
cursor.execute(sql)
cursor.commit()
