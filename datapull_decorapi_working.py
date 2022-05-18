# -*- coding: utf-8 -*-
"""
Created on Tue May 17 02:35:12 2022

@author: nbadam
"""

# -*- coding: utf-8 -*-
"""
Created on Tue May 17 02:20:57 2022

@author: nbadam
"""


import os
#from pyforest import *
import numpy as np
import pandas as pd
#os.chdir('C:/Users/nbadam/Personicle/apple_health_export/backend-data-api-v2-main')
#from flask import *
from flask import Flask
from flask import request
#from flask_restful import Resource, Api, reqparse
#import ast
from sqlalchemy import select
import pandas.io.sql as sqlio
from base_schema import *
from db_connection import *
import json,time
from pandarallel import pandarallel
pandarallel.initialize()


app = Flask(__name__)
#api = Api(app)


@app.route('/',methods=['GET'])

def home_page():
    
    data_set={'Page':'Home','Message':'Succesful load','Timestamp':time.time()}
    json_dump= json.dumps(data_set)
    
    return json_dump



@app.route('/user/',methods=['GET'])

def request_page():
    
    tblname  = request.args.get('tblname', type=str ,default='')
    source  = request.args.get('source',type=str , default='')
    ts  = request.args.get('ts',type=str , default='')
    te  = request.args.get('te',type=str , default='')
    freq  = request.args.get('freq',type=str , default='')

    query_hr='select * from '+ tblname
    
    
    hr= sqlio.read_sql_query(query_hr,engine)
    hr=hr[(hr.source==source) &(pd.to_datetime(hr.timestamp)>=ts)&(pd.to_datetime(hr.timestamp)<=te)].copy()
    
    # pd.to_datetime(hr.timestamp)
     
    hr['startdate']=hr['timestamp']-pd.Timedelta('1min')
    hr.rename(columns={'timestamp':'enddate','source':'sourcename'},inplace=True)
    hr=hr[['individual_id','startdate','enddate', 'sourcename', 'value', 'unit', 'confidence']].copy()
    
    
    
    hr.drop_duplicates(inplace=True)
    hr.reset_index(inplace=True)
    hr.rename(columns={'index':'ind'},inplace=True)
    hr['diff_seconds'] = hr['enddate'] - hr['startdate']
    hr['diff_seconds']=hr['diff_seconds']/np.timedelta64(1,'s')
    hr['weight']=hr['diff_seconds']**(-1)
    hr.weight.fillna(1,inplace=True)
    
         
    import pandas as pd1
    
    hr['startdate']=hr.parallel_apply(lambda d:pd1.date_range(d['startdate'],d['enddate']-pd1.Timedelta(freq) if d['diff_seconds'] > 0 else d['startdate'],freq=freq),axis=1)
    hr=hr.explode('startdate')
    hr['enddate']=(hr['startdate']+pd1.Timedelta(freq)).dt.ceil('1min')
    hr['wt_value']=hr['weight']*hr['value']
    hr=hr.pivot_table(index=['individual_id','startdate','enddate'],columns='sourcename',values=['wt_value','weight']).reset_index()
    hr.columns = list(map("_".join, hr.columns))
    hr.rename(columns={'individual_id_':'individual_id','startdate_':'startdate','enddate_':'enddate'},inplace=True)
    hr.fillna(0,inplace=True)
    
    
    
    # hr['startdate']=hr.parallel_apply(lambda d:pd1.date_range(d['startdate'],d['enddate']-pd1.Timedelta('1min') if d['diff_seconds'] > 0 else d['startdate']),axis=1)
    # hr=hr.explode('startdate')
    # hr['enddate']=(hr['startdate']+pd1.Timedelta('1min')).dt.ceil('1min')
    # hr['wt_value']=hr['weight']*hr['value']
    # hr=hr.pivot_table(index=['individual_id','startdate','enddate'],columns='sourcename',values=['wt_value','weight']).reset_index()
    # hr.columns = list(map("_".join, hr.columns))
    # hr.rename(columns={'individual_id_':'individual_id','startdate_':'startdate','enddate_':'enddate'},inplace=True)
    # hr.fillna(0,inplace=True)
    
    hr['result_stream']=np.round((hr.loc[:,hr.columns.str.startswith("wt_value")].sum(axis=1))/(hr.loc[:,hr.columns.str.startswith("weight")].sum(axis=1)))



       
    for col in ('startdate','enddate'):    
         hr[col] = hr[col].dt.strftime('%Y-%m-%d %H:%M:%S')
   
    
    hr=hr.head(5).copy()
    
    #data=hr.to_dict('index')
    data=hr.to_json(orient='records')
    
    #data= hr.to_json()
    #data=hr.to_dict()
    
    #data1=[]
    #for i in data:
    #    data1.append(i)
    
    print(hr)
    #print(data1)
    return data

if __name__ == '__main__':
     app.run(port=7777)



