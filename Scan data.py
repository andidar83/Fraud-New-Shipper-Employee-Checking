# -*- coding: utf-8 -*-
"""
Created on Mon Oct 16 15:15:46 2023

@author: NXP
"""
import pyodbc
import sqlalchemy
import os
import requests
import time
from pprint import pprint
import json
import pandas as pd
import sys
from datetime import datetime,timedelta
start = datetime.now()
import numpy as np
import gspread as gs
import df2gspread as d2g
import gspread_dataframe as gd
import urllib
import time
from datetime import datetime

credentials ={

}
gc = gs.service_account_from_dict(credentials)


def export_to_sheets(file_name,sheet_name,mode='r'):
    ws = gc.open(file_name).worksheet(sheet_name)
    if(mode=='r'):
        return gd.get_as_dataframe(worksheet=ws)

def export_to_sheets2(file_name,sheet_name,df,mode='r'):
    ws = gc.open(file_name).worksheet(sheet_name)
    if(mode=='w'):
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)
        return True
    elif(mode=='a'):
        #ws.add_rows(4)
        old = gd.get_as_dataframe(worksheet=ws)
        updated = pd.concat([old,df])
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=updated,include_index=False,include_column_header=True,resize=True)
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)
    
    

conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=(local);'
    r'DATABASE=Fraud_Screening;'
    r'Trusted_Connection=yes;'
)
cnxn = pyodbc.connect(conn_str)

cursor = cnxn.cursor()
cursor.fast_executemany = True








#Scan Shipper####################################################################

query = '''
SELECT [legacy_id]
      ,LOWER(SUBSTRING(
        [shipper_name],
        CHARINDEX('(', [shipper_name]) + 1,
        LEN([shipper_name]) - CHARINDEX('(', [shipper_name]) - 1
    )) AS name 
      ,LOWER([shipper_email]) AS email
      ,[shipper_contact] as contact
  FROM [Fraud_Screening].[dbo].[Fraud_Shipper_Table]
'''
query_shipper2 = '''
SELECT 
	  lower(shipper_address) as address
  FROM [Fraud_Screening].[dbo].[Fraud_Shipper_Table]
'''

database = pd.read_sql(query, cnxn)
databaseshipper2 = pd.read_sql(query_shipper2, cnxn)
database = database[~database.duplicated(keep='first')]
databaseshipper2 = databaseshipper2 [~databaseshipper2 .duplicated(keep='first')]




database['name_check'] = 'found in frd shipper database'
database['email_check'] = 'found in frd shipper database'
database['contact_check'] = 'found in frd shipper database'
databaseshipper2['address_check'] = 'found in frd shipper database'

##################################################################################
query2 = '''
SELECT 
  LOWER(LTRIM(RTRIM(SUBSTRING(name, CHARINDEX('@', name) + 1, LEN(name))))) as name
 	  ,[contact_no] as contact
      ,lOWER([email]) as email
      ,[Bank_Account_number]
      ,LOWER([Bank]) as Bank
      ,lower([Owner_name]) as Owner_Name
  FROM [Fraud_Screening].[dbo].[Fraud_Mitra_Table]
'''

query_mitra2 = '''
SELECT 
      lower([Address]) as address

  FROM [Fraud_Screening].[dbo].[Fraud_Mitra_Table]

'''
database_mitra = pd.read_sql(query2, cnxn)
databasemitra2 = pd.read_sql(query_mitra2, cnxn)
database_mitra = database_mitra[~database_mitra.duplicated(keep='first')]
databasemitra2 = databasemitra2 [~databasemitra2 .duplicated(keep='first')]


database_mitra['name_check'] = 'found in frd mitra database'
database_mitra['email_check'] = 'found in frd mitra database'
database_mitra['contact_check'] = 'found in frd mitra database'
database_mitra['bank_acc_check'] = 'found in frd mitra database'
database_mitra['owner_check'] = 'found in frd mitra database'
databasemitra2['address_check'] = 'found in frd mitra database'

database_names = pd.concat([database[['name','name_check']],database_mitra[['name','name_check']]])
database_emails = pd.concat([database[['email','email_check']],database_mitra[['email','email_check']]])
database_contact = pd.concat([database[['contact','contact_check']],database_mitra[['contact','contact_check']]])
database_address = pd.concat([databaseshipper2 [['address','address_check']],databasemitra2[['address','address_check']]])

###########################################################################################


new_shipper = export_to_sheets('fraud prototype','New Shipper','r')
new_shipper = new_shipper[new_shipper['name'].notna()]
new_shipper = new_shipper[['name','email','contact','address']]

new_shipper['name'] = new_shipper['name'].str.lower()
new_shipper['email'] = new_shipper['email'].str.lower()
new_shipper['address'] = new_shipper['address'].str.lower()


cek_name = pd.merge(new_shipper,database_names,on = ["name"],how = 'left')
cek_email = pd.merge(cek_name,database_emails,on = ["email"],how = 'left')
cek_contact = pd.merge(cek_email ,database_contact,on = ["contact"],how = 'left')
cek_address = pd.merge(cek_contact,database_address[['address','address_check']],on = ['address'],how = 'left')
cek_address['Fraud(Yes/No)'] =  np.where(cek_address[['name_check', 'email_check', 'contact_check','address_check']].notna().any(axis=1), 'fraud', 'not fraud')
cek_address['last refreshed'] = datetime.now()
print(cek_address.info())
out = export_to_sheets2('fraud prototype','New Shipper',cek_address,'w')



#Scan Mitra####################################################################



new_mitra = export_to_sheets('fraud prototype','New Mitra','r')
new_mitra= new_mitra[new_mitra['name'].notna()]
new_mitra = new_mitra[['name','contact','email','address','Bank_Account_number','Owner_Name']]

new_mitra['name'] = new_mitra['name'].str.lower()
new_mitra['email'] = new_mitra['email'].str.lower()
new_mitra['Owner_Name'] = new_mitra['Owner_Name'].str.lower()
new_mitra['address'] = new_mitra['address'].str.lower()
print(new_mitra.info())




cek_mitra_name = pd.merge(new_mitra,database_names,on = ['name'],how = 'left')
cek__mitra_contact = pd.merge(cek_mitra_name,database_contact,on = ['contact'],how = 'left')
cek__mitra_email = pd.merge(cek__mitra_contact,database_emails,on = ['email'],how = 'left')
cek__mitra_bank_acc = pd.merge(cek__mitra_email,database_mitra[['Bank_Account_number','bank_acc_check']],on = ['Bank_Account_number'],how = 'left')
cek__mitra_owner = pd.merge(cek__mitra_bank_acc,database_mitra[['Owner_Name','owner_check']],on = ['Owner_Name'],how = 'left')
cek__mitra_address = pd.merge(cek__mitra_owner,database_address,on = ['address'],how = 'left')



cek__mitra_address['Fraud(Yes/No)'] =  np.where(cek__mitra_address[['name_check','email_check','contact_check','bank_acc_check','owner_check']].notna().any(axis=1), 'fraud', 'not fraud')
cek__mitra_address['last refreshed'] = datetime.now()
out_mitra = export_to_sheets2('fraud prototype','New Mitra',cek__mitra_address,'w')
print(cek__mitra_owner.info())





