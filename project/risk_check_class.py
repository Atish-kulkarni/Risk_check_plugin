# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 13:43:29 2022

@author: atish
"""

import os 
from difflib import SequenceMatcher
import numpy as np 
from datetime import datetime
import pandas as pd
from typing import Union
import redshift_connector
from pydantic import BaseModel
from fastapi.testclient import TestClient
from fastapi import FastAPI, HTTPException


cwd = os.getcwd()
os.chdir(cwd)

df = pd.read_csv('df_base.csv')

sub_df = df[df['Customer Email'] == 'Aziznono94@gmail.com']
email = "'Aziznono94@gmail.com'"
shopper_name = 'Atish'
dt = datetime.now()
transactions_dict = df.to_dict('index')


app=FastAPI()

class transaction():

    def __init__(self, shopper_email, shopper_name, transactions_dict, dt, df, outgoing_response, label,
                 historic_check ):
        self.shopper_email = shopper_email
        self.shopper_name = shopper_name
        self.transactions_dict = transactions_dict
        self.dt = dt
        self.df = df
        self.outgoing_response = outgoing_response
        self.points = {}
        self.label = label
        self.historic_fraud_check = historic_check
        self.df_whitelist = pd.read_csv('whitelist.txt', encoding= 'unicode_escape', sep='\t')
        self.df_blacklist = pd.read_csv('blacklist.txt', encoding= 'unicode_escape', sep='\t')
    def shopper_name_email_comparison(self ):
        score = SequenceMatcher(None, self.shopper_name.lower(),self.shopper_email.split('@')[0]).ratio()
        self.points["shopper_name_email_comparison"] = 10
        return round(score,2)

    def shopper_email_domain(self):
        try:
            return self.shopper_email.split('@')[1]
        except:
            return -1
        
    def time_diff_email(self):
        try:
            
            str_dt =  list(df[df['Customer Email'] == self.shopper_email]['timestamps'])[-1]
            
            if pd.Timedelta(self.dt - pd.to_datetime(str_dt)).seconds < 10:
                self.points["time_diff_email"] = 10
            return pd.Timedelta(self.dt - pd.to_datetime(str_dt)).seconds
        except:
            return -1
        
    def time_diff_email_riskcheck(self):
        try:
            df_check = pd.DataFrame(self.outgoing_response)
            str_dt =  max(df_check[df_check['shopper_email'] == self.shopper_email]['timestamp_risk_check'])
            if pd.Timedelta(self.dt - pd.to_datetime(str_dt)).seconds < 10:
                self.points["time_diff_email_riskcheck"] = 10
            
            return pd.Timedelta(self.dt - pd.to_datetime(str_dt)).seconds
        except:
            return -1
    
    
    def previous_timestamp_email_riskcheck(self):
        try:
            df_check = pd.DataFrame(self.outgoing_response)
            str_dt =  max(df_check[df_check['shopper_email'] == self.shopper_email]['timestamp_risk_check'])
            
            return str_dt
        except:
            return -1
    
    
    def total_points(self):
        try:
            return sum(self.points.values())
        except:
            return -1
        
    
    def connect_dwh_get_fraud (self):
        try:
            if self.historic_fraud_check == True:
                conn = redshift_connector.connect(
                  host='dp-production.cayyhvdehckl.eu-west-1.redshift.amazonaws.com',
                  database='datawarehouse',
                  user='akulkarni',
                  password='rG4QcUwZthKIHGYMHNnkJryuMiH/Nd2+aJWzwDE3pqJodhmLvhHIZojfTqBLUY7ndbN9zlQ==')
                email = self.shopper_email
                email = "'" + self.shopper_email + "'"
                querry_get_fraud = "SELECT historical.cgpay_pay_paymethod_adyen_abstract_fraud.psp_reference,\
                   historical.cgpay_pay_paymethod_adyen_abstract_fraud.original_reference, \
                   historical.adyen_payment.psp_reference,historical.adyen_payment.shopper_email, \
                   historical.adyen_payment.creation_date, \
                   historical.cgpay_pay_paymethod_adyen_abstract_fraud.updated_at \
                   From historical.cgpay_pay_paymethod_adyen_abstract_fraud \
                   inner join historical.adyen_payment \
                   on historical.cgpay_pay_paymethod_adyen_abstract_fraud.original_reference =  historical.adyen_payment.psp_reference\
                   where  historical.adyen_payment.shopper_email  = " + email + "\
                   ORDER BY historical.cgpay_pay_paymethod_adyen_abstract_fraud.updated_at desc"
                df = pd.read_sql(querry_get_fraud, conn)
                conn.close()
        
            return len(df)
        except:
            return -1
    
        
    def test(self):
        return 'git test complete 2'
    
    def member_whitelist(self):
         return self.shopper_email in self.df_whitelist['Customer Email'].values
     
    def member_blacklist(self):
         return self.shopper_email in self.df_blacklist['Customer Email'].values
        