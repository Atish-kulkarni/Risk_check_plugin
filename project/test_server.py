# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 09:53:03 2022

@author: atish
"""



import requests
import pandas as pd
import io
import json
from pandas.io.json import json_normalize
import os
import time

cwd = os.getcwd()
os.chdir(cwd)




url = "http://127.0.0.1:8000/shopper_check"
url = "http://52.59.153.134:80/shopper_check"
for i in range(1):
    myobj = {
        "Shopper_email": "atish.k3@gmail.com",
        "Shopper_name" : "mike",
        "trx_value": "4",
        "label": "GMP",
        "historic_check": False}
    
    start_time = time.time()
    response = requests.post(url, json=myobj)
    print("--- %s seconds ---" % (time.time() - start_time))
    print(response.content)
    res = pd.json_normalize(response.json()).T


import pickle

with open('incoming_database.pickle', 'rb') as in_db:
    df_in_db = pickle.load(in_db)

with open('outgoing_database.pickle', 'rb') as out_db:
    df_out_db = pickle.load(out_db)

df_out = pd.DataFrame(df_out_db)