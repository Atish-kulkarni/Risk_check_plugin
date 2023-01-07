
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 11:57:24 2022

@author: atish
"""

from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import codecs
import pandas as pd
import pickle
import os
from typing import Union
from fastapi.encoders import jsonable_encoder
import numpy as np
from risk_check_class import transaction
from datetime import datetime
from starlette.responses import FileResponse 
cwd = os.getcwd()
os.chdir(cwd)

app = FastAPI()

df = pd.DataFrame()


transactions_dict = df.to_dict('index')

df_whitelist = pd.read_csv('whitelist.txt', encoding= 'unicode_escape', sep='\t')





class Shopper(BaseModel):
    Shopper_email: Union[str, None] = None
    Shopper_name: Union[str, None] = None
    trx_value: Union[float, None] = None
    label: Union[str, None] = None
    historic_check: Union[bool, None] = None
    
@app.get("/")
async def root():
    return FileResponse('image.jpg')

@app.post('/shopper_check')
def trx_check(current_shopper: Shopper):
    """
    Loads previous databases in case of server failure 
    """

    try: 
        with open('C:/API data/outgoing_database.pickle', 'rb') as out_db:
            outgoing_response = pickle.load(out_db)
    except:
        outgoing_response = []
        
    try:
        with open('C:/API data/incoming_database.pickle', 'rb') as in_db:
            incoming_requests = pickle.load(in_db)
    except:
        incoming_requests = []
    
        
    """ 
    Relates to the import of transaction from risk check class 
    initialize the inputs here 
    """
    dt_risk_check = datetime.now()
    
    current_trasnaction = transaction(
        shopper_email=current_shopper.Shopper_email,
        shopper_name=current_shopper.Shopper_name,
        transactions_dict=transactions_dict, 
        dt=dt_risk_check,
        df=df,
        outgoing_response = outgoing_response,
        label=current_shopper.label,
        historic_check = current_shopper.historic_check )
    
   
    """
    This function creates incoming requests database 
    """
    
    json_compatible_item_data = jsonable_encoder(current_shopper)
    incoming_requests.append(json_compatible_item_data)
    
    with open('C:/API data/incoming_database.pickle', 'wb') as in_db:
        pickle.dump(incoming_requests, in_db)
    
    
    """
    This function creates response and all the checks performed should
    written here 
    """ 
    response = {
            'shopper_email': current_shopper.Shopper_email, 
            'shopper_name': current_shopper.Shopper_name, 
            "shopper_name_email_comparison":current_trasnaction.shopper_name_email_comparison(),
            "shopper_domain":current_trasnaction.shopper_email_domain(),
            "time_diff_email": current_trasnaction.time_diff_email(),
            "value": current_shopper.trx_value,
            "timestamp_risk_check" : dt_risk_check,
            "time_diff_email_riskcheck" :  current_trasnaction.time_diff_email_riskcheck(),
            "points": current_trasnaction.points,
            "points_total" : current_trasnaction.total_points(),
            "label" : current_shopper.label,
            "NOF_total" : current_trasnaction.connect_dwh_get_fraud(),
            "member_whitelist" : current_trasnaction.member_whitelist(),
            "member_blacklist" : current_trasnaction.member_blacklist(),
            "test": current_trasnaction.test(),
            "server_pushed": 'From AWS'
            }
    with open('filename.pickle', 'wb') as handle:
        pickle.dump(response, handle)
        
        
    """
    This function creates outgoing requests database 
    """
    outgoing_response.append(response)
    with open('C:/API data/outgoing_database.pickle', 'wb') as out_db:
        pickle.dump(outgoing_response, out_db)
    try:
        return response
    except:
        return 'API failed'

if __name__ == "__main__":
    uvicorn.run('main:app', host="127.0.0.1", port=8000, reload=True, log_level='trace', use_colors=True)
    