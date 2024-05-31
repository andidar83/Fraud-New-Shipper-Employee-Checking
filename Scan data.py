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



def fraud_database():
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        r'SERVER=(local);'
        r'uid=sqladmin;'
        r'pwd=indatawetrust;'
        r'DATABASE=Fraud;'
        r'Trusted_Connection=no;'
    )
    cnxn = pyodbc.connect(conn_str)
    
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    
    #Scan Shipper####################################################################
    
    # query = '''
    # SELECT [legacy_id]
    #       ,LOWER(SUBSTRING(
    #         [shipper_name],
    #         CHARINDEX('(', [shipper_name]) + 1,
    #         LEN([shipper_name]) - CHARINDEX('(', [shipper_name]) - 1
    #     )) AS name 
    #       ,LOWER(trim([shipper_email])) AS email
    #       ,[shipper_contact] as contact
    #   FROM [Fraud].[dbo].[frd_shipper]
    # '''
    
    query = '''
    SELECT [legacy_id]
          ,LOWER(
            [shipper_name]
            ) AS name 
          ,LOWER(trim([shipper_email])) AS email
          ,[shipper_contact] as contact
      FROM [Fraud].[dbo].[frd_shipper]
    '''
    query_shipper_fin_kyc = '''
    SELECT [legacy_id]
          ,LOWER(
            [shipper_name]
            ) AS name 
          ,LOWER(trim([shipper_email])) AS email
          ,[shipper_contact] as contact
      FROM [Fraud].[dbo].[frd_shipper_finance]
    '''
    query_shipper2 = '''
    SELECT 
          lower([billing_address]) as address
      
    
      FROM [Fraud].[dbo].[frd_shipper] where [billing_address] is not null
      union
      SELECT 
        lower([shipper_address]) as address
    
      FROM [Fraud].[dbo].[frd_shipper]  where [shipper_address] is not null
    '''
    
    
    database = pd.read_sql(query, cnxn)
    database_finkyc = pd.read_sql(query_shipper_fin_kyc, cnxn)
    databaseshipper2 = pd.read_sql(query_shipper2, cnxn)
    database = database[~database.duplicated(keep='first')]
    database_finkyc =  database_finkyc[~ database_finkyc.duplicated(keep='first')]
    databaseshipper2 = databaseshipper2 [~databaseshipper2 .duplicated(keep='first')]
    
    
    
    
    database['name_check'] = 'found in frd shipper database'
    database['email_check'] = 'found in frd shipper database'
    database['contact_check'] = 'found in frd shipper database'
    database_finkyc['name_check'] = "found in shipper kyc database"
    database_finkyc['email_check'] = "found in shipper kyc database"
    database_finkyc['contact_check'] = "found in shipper kyc database"
    
    database = pd.concat([database,database_finkyc])
    databaseshipper2['address_check'] = 'found in frd shipper database'
    
    ##################################################################################
    query2 = '''
      SELECT 
      LOWER(LTRIM(RTRIM(SUBSTRING(name, CHARINDEX('@', name) + 1, LEN(name))))) as name
     	  ,[contact_no] as contact
          ,lOWER(trim([email])) as email
          ,[Bank_Account_number]
          ,LOWER([Bank]) as Bank
          ,lower(trim([Owner_name])) as Owner_Name
      FROM  [Fraud].[dbo].[frd_mitra]
    '''
    
    query_mitra2 = '''
      SELECT 
          lower([Address]) as address
    
      FROM [Fraud].[dbo].[frd_mitra]
    '''
    
    query_mitra3 = '''
      SELECT 
          lower([Owner_name]) as name
    
      FROM [Fraud].[dbo].[frd_mitra]
    '''
    
    database_mitra = pd.read_sql(query2, cnxn)
    databasemitra2 = pd.read_sql(query_mitra2, cnxn)
    databasemitraowner = pd.read_sql(query_mitra3, cnxn)
    database_mitra = database_mitra[~database_mitra.duplicated(keep='first')]
    databasemitra2 = databasemitra2 [~databasemitra2 .duplicated(keep='first')]
    databasemitraowner = databasemitraowner[~databasemitraowner.duplicated(keep='first')]
    
    
    database_mitra['name_check'] = 'found in frd mitra database'
    database_mitra['email_check'] = 'found in frd mitra database'
    database_mitra['contact_check'] = 'found in frd mitra database'
    database_mitra['bank_acc_check'] = 'found in frd mitra database'
    
    database_mitra['owner_check'] = 'found in frd mitra database'
    databasemitra2['address_check'] = 'found in frd mitra database'
    databasemitraowner['name_check'] = 'found in frd mitra database'
    
    
    
    #########################################################################################################################
    
    query_consignee1 = '''
    SELECT lower(trim([consignee_name])) as name
          ,lower(trim([consignee_email])) as email
          ,[consignee_contact] as contact
      FROM [Fraud].[dbo].[frd_consignee]
    '''
    
    query_consignee2 = '''
    SELECT 
          lower([consignee_address1]) as address
        
    
      FROM [Fraud].[dbo].[frd_consignee]
    
      union
    
      SELECT 
      
          lower([consignee_address2]) as address
    
      FROM [Fraud].[dbo].[frd_consignee]
    '''
    
    database_consignee = pd.read_sql(query_consignee1, cnxn)
    database_consignee2 = pd.read_sql(query_consignee2, cnxn)
    
    database_consignee = database_consignee[~database_consignee.duplicated(keep='first')]
    database_consignee2 = database_consignee2[~database_consignee2.duplicated(keep='first')]
    
    database_consignee['name_check'] = 'found in frd consignee database'
    database_consignee['email_check'] = 'found in frd consignee database'
    database_consignee['contact_check'] = 'found in frd consignee database'
    database_consignee2['address_check'] = 'found in frd consignee database'
    
    
    ###############################################################################################################################
    query_karyawan = '''
    SELECT  lower(trim([Nama Karyawan])) as name
          ,[Nomor KTP] as NIK
      FROM [Fraud].[dbo].[frd_karyawan]
    '''
    database_karyawan = pd.read_sql(query_karyawan, cnxn)
    database_karyawan = database_karyawan[~database_karyawan.duplicated(keep='first')]
    
    database_karyawan['name_check'] = 'found in karyawan database'
    database_karyawan['NIK_check'] =  'found in karyawan database'
    
    
    ################################################################################################################################
    query_driver = '''
    SELECT 
      lower([Employee_Name]) as name
      ,[NIK_KTP] as NIK
      ,[Employee_Contact] as contact
      ,lower([Employee_Email]) as email
      ,[Employee_Address]  as address
      ,[Employee_Bank_Account] as Bank_Account_number
      FROM [Fraud].[dbo].[Driver_table]
    '''
    
    database_driver =  pd.read_sql(query_driver, cnxn)
    database_driver = database_driver[~database_driver.duplicated(keep='first')]
    database_driver['name_check'] = 'found in driver database'
    database_driver['NIK_check'] =  'found in driver database'
    database_driver['email_check'] = 'found in driver database'
    database_driver['contact_check'] = 'found in driver database'
    database_driver['bank_acc_check'] = 'found in driver database'
    
    
    ###############################################################################################################################
    
    #ALL FRD DATAS FROM ALL TABLE
    database_names = pd.concat([database[['name','name_check']],database_mitra[['name','name_check']],databasemitraowner[['name','name_check']],database_consignee[['name','name_check']],database_karyawan[['name','name_check']],database_driver[['name','name_check']]])
    database_emails = pd.concat([database[['email','email_check']],database_mitra[['email','email_check']],database_consignee[['email','email_check']],database_driver[['email','email_check']]])
    database_contact = pd.concat([database[['contact','contact_check']],database_mitra[['contact','contact_check']],database_consignee[['contact','contact_check']],database_driver[['contact','contact_check']]])
    database_address = pd.concat([databaseshipper2 [['address','address_check']],databasemitra2[['address','address_check']],database_consignee2[['address','address_check']]])
    database_bank_acc = pd.concat([database_mitra[['Bank_Account_number','bank_acc_check']],database_driver[['Bank_Account_number','bank_acc_check']]])
    database_NIK = pd.concat([database_karyawan[['NIK','NIK_check']],database_driver[['NIK','NIK_check']]])
    
    
    database_names = database_names[~database_names.duplicated(keep='first')]
    database_emails = database_emails[~database_emails.duplicated(keep='first')]
    database_contact = database_contact[~database_contact.duplicated(keep='first')]
    database_address = database_address[~database_address.duplicated(keep='first')]
   
        
    database_names = database_names.dropna(how='any',axis=0)
    database_emails = database_emails.dropna(how='any',axis=0)
    database_contact = database_contact.dropna(how='any',axis=0)
    database_address = database_address.dropna(how='any',axis=0)
    database_bank_acc = database_bank_acc.dropna(how='any',axis=0)
    database_NIK = database_NIK.dropna(how='any',axis=0)
    
    print(database_names)

    return [database_names,database_emails,database_contact,database_address,database_bank_acc,database_NIK]





fraud_database = fraud_database()


#SHIPPER ###########################################################################################

trigger = 1
while trigger == 1:
    while True:
        try:
    
            button1 =  export_to_sheets('FBS','New Shipper','r')[:0].columns[1]
            button2 =  export_to_sheets('FBS','New Mitra','r')[:0].columns[1]
            button3 =  export_to_sheets('FBS','New Employee','r')[:0].columns[1]
        
         
         
                   
            if button1 == 'YES':
                ws = gc.open('FBS').worksheet('New Shipper')
                ws.update('b1','RUNNING', raw=False)
                
                
                #########################################################################################################
                new_shipper = export_to_sheets('FBS','New Shipper','r')
                new_shipper.columns = new_shipper.iloc[0]
                new_shipper = new_shipper[1:]
        
                # new_shipper = new_shipper[new_shipper['name'].notna()]
                new_shipper = new_shipper[['name','email','contact','address','Bank_Account_number','NIK']]
        
                new_shipper['name'] = new_shipper['name'].str.lower()
                new_shipper['email'] = new_shipper['email'].str.lower()
                new_shipper['address'] = new_shipper['address'].str.lower()
        
                
                print(new_shipper['name'])
                
                cek_name = pd.merge(new_shipper,fraud_database[0],on = ["name"],how = 'left')
                cek_email = pd.merge(cek_name,fraud_database[1],on = ["email"],how = 'left')
                cek_contact = pd.merge(cek_email ,fraud_database[2],on = ["contact"],how = 'left')
                cek_address = pd.merge(cek_contact,fraud_database[3],on = ['address'],how = 'left')
                cek_bank_acc = pd.merge(cek_address,fraud_database[4],on=['Bank_Account_number'],how='left')
                cek_nik = pd.merge(cek_bank_acc,fraud_database[5],on=['NIK'],how='left')
                cek_nik['Fraud(Yes/No)'] =  np.where(cek_nik[['name_check', 'email_check', 'contact_check','address_check','bank_acc_check','NIK_check']].notna().any(axis=1), 'fraud', 'not fraud')
                cek_nik['last refreshed'] = str(datetime.now())
                cek_nik = cek_nik.fillna('')
                cek_nik = cek_nik[['name','email','contact','address','Bank_Account_number','NIK','name_check','email_check','contact_check','address_check','bank_acc_check','NIK_check','Fraud(Yes/No)','last refreshed']]
                
                # out = export_to_sheets2('Fraud Blacklist Screening','New Shipper',cek_address,'w')
                cek_nik=cek_nik.astype(str)
                print('updateing values')
                ws.update('A2',[cek_nik.columns.values.tolist()] + cek_nik.values.tolist(), raw=False)
        
                print("Done")
                ################################################################################################################################################
            
                
        
                ws.update('b1','NO', raw=False)
            elif button2 == 'YES':
                ws2 = gc.open('FBS').worksheet('New Mitra')
                ws2.update('b1','RUNNING', raw=False)
                
                
                #########################################################################################################
                new_mitra = export_to_sheets('FBS','New Mitra','r')
                new_mitra.columns = new_mitra.iloc[0]
                new_mitra = new_mitra[1:]
        
                # new_mitra  = new_mitra[new_mitra['name'].notna()]
                new_mitra = new_mitra[['name','email','contact','address','Bank_Account_number','NIK']]
        
                new_mitra['name'] = new_mitra['name'].str.lower()
                new_mitra['email'] = new_mitra['email'].str.lower()
                new_mitra['address'] = new_mitra['address'].str.lower()
        
                
                cek_name2 = pd.merge(new_mitra,fraud_database[0],on = ["name"],how = 'left')
                cek_email2 = pd.merge(cek_name2,fraud_database[1],on = ["email"],how = 'left')
                cek_contact2 = pd.merge(cek_email2,fraud_database[2],on = ["contact"],how = 'left')
                cek_address2 = pd.merge(cek_contact2,fraud_database[3],on = ['address'],how = 'left')
                cek_bank_acc2 = pd.merge(cek_address2,fraud_database[4],on=['Bank_Account_number'],how='left')
                cek_nik2 = pd.merge(cek_bank_acc2,fraud_database[5],on=['NIK'],how='left')
                cek_nik2['Fraud(Yes/No)'] =  np.where(cek_nik2[['name_check', 'email_check', 'contact_check','address_check','bank_acc_check','NIK_check']].notna().any(axis=1), 'fraud', 'not fraud')
                cek_nik2['last refreshed'] = str(datetime.now())
                cek_nik2 = cek_nik2.fillna('')
                print(cek_nik2.info())
                
                cek_nik2 = cek_nik2[['name','email','contact','address','Bank_Account_number','NIK','name_check','email_check','contact_check','address_check','bank_acc_check','NIK_check','Fraud(Yes/No)','last refreshed']]
                
                # out = export_to_sheets2('Fraud Blacklist Screening','New Shipper',cek_address,'w')
                cek_nik2=cek_nik2.astype(str)
                ws2.update('A2',[cek_nik2.columns.values.tolist()] + cek_nik2.values.tolist(), raw=False)
        
                print("Done")
                ################################################################################################################################################
            
                
        
                ws2.update('b1','NO', raw=False)
                
            elif button3 =='YES':
                ws3 = gc.open('FBS').worksheet('New Employee')
                ws3.update('b1','RUNNING', raw=False)
                
                
                #########################################################################################################
                new_employee = export_to_sheets('FBS','New Employee','r')
        
                new_employee.columns = new_employee.iloc[0]
        
                new_employee = new_employee[1:]
              
        
                # new_employee = new_employee[new_employee['name'].notna()]
                new_employee = new_employee[['name','email','contact','address','Bank_Account_number','NIK']]
        
                new_employee['name'] = new_employee['name'].str.lower()
                new_employee['email'] = new_employee['email'].str.lower()
                new_employee['address'] = new_employee['address'].str.lower()
                
                
                cek_name3 = pd.merge(new_employee,fraud_database[0],on = ["name"],how = 'left')
                
                cek_email3 = pd.merge(cek_name3,fraud_database[1],on = ["email"],how = 'left')
                cek_contact3 = pd.merge(cek_email3,fraud_database[2],on = ["contact"],how = 'left')
                cek_address3 = pd.merge(cek_contact3,fraud_database[3],on = ['address'],how = 'left')
                cek_bank_acc3 = pd.merge(cek_address3,fraud_database[4],on=['Bank_Account_number'],how='left')
                cek_nik3 = pd.merge(cek_bank_acc3,fraud_database[5],on=['NIK'],how='left')
                cek_nik3['Fraud(Yes/No)'] =  np.where(cek_nik3[['name_check', 'email_check', 'contact_check','address_check','bank_acc_check','NIK_check']].notna().any(axis=1), 'fraud', 'not fraud')
                cek_nik3['last refreshed'] = str(datetime.now())
                cek_nik3 = cek_nik3.fillna('')
                cek_nik3 = cek_nik3[['name','email','contact','address','Bank_Account_number','NIK','name_check','email_check','contact_check','address_check','bank_acc_check','NIK_check','Fraud(Yes/No)','last refreshed']]
                
               
        
                # out = export_to_sheets2('Fraud Blacklist Screening','New Shipper',cek_address,'w')
                cek_nik3=cek_nik3.astype(str)
                print(cek_nik3.columns.values.tolist())
                ws3.update('A2',[cek_nik3.columns.values.tolist()] + cek_nik3.values.tolist(), raw=False)
        
                print("Done")
                ################################################################################################################################################
            
                
        
                ws3.update('b1','NO', raw=False)
                
                
            
            time.sleep(9)
            print('stand_by...')
            break
        except:
            print('>> Pulling failed, retrying...')


