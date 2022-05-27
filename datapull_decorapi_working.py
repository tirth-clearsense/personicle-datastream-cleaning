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
import numpy as np
import pandas as pd
from flask import Flask
from flask import request
from sqlalchemy import select
import pandas.io.sql as sqlio
from base_schema import *
from db_connection import *
import json,time
from pandarallel import pandarallel
from config import DATA_SYNC_CONFIG
pandarallel.initialize()
from producer.send_datastreams_to_azure import datastream_producer

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
app = Flask(__name__)

@app.route('/fetch_data/',methods=['GET'])
def request_page():
    
    user  = request.args.get('user',type=str , default='')
    tblname  = request.args.get('data', type=str ,default='')
    source  = request.args.get('source',type=str , default='')
    sources = source.split(";") if source is not None else None
    #print("sources:",sources)
    ts  = request.args.get('starttime',type=str , default='')
    te  = request.args.get('endtime',type=str , default='')
    freq  = request.args.get('freq',type=str , default='')
    query_hr='select * from '+ tblname
    
    hr= sqlio.read_sql_query(query_hr,engine)
    hr['source']=np.where(pd.to_datetime(hr.timestamp)=='2021-12-09','connect','google-fit')
    hr=hr[(hr.individual_id==user)&(hr.source.isin(sources))&(pd.to_datetime(hr.timestamp)>=ts)&(pd.to_datetime(hr.timestamp)<=te)].copy()
     
    hr['startdate']=hr['timestamp']-pd.Timedelta('1min')
    hr.rename(columns={'timestamp':'enddate','source':'sourcename'},inplace=True)
    hr=hr[['individual_id','startdate','enddate', 'sourcename', 'value', 'unit', 'confidence']].copy()
    
    hr.drop_duplicates(inplace=True)
    hr.reset_index(inplace=True)
    hr.rename(columns={'index':'ind'},inplace=True)
    hr['diff_seconds'] = hr['enddate'] - hr['startdate']
    hr['diff_seconds']=hr['diff_seconds']/np.timedelta64(1,'s')
    hr['weight']=np.round(hr['diff_seconds']**(-1),4)
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
    
    hr['result_stream']=np.round((hr.loc[:,hr.columns.str.startswith("wt_value")].sum(axis=1))/(hr.loc[:,hr.columns.str.startswith("weight")].sum(axis=1)))

    for col in ('startdate','enddate'):    
         hr[col] = hr[col].dt.strftime('%Y-%m-%d %H:%M:%S')
   
    hr=hr.loc[:,~hr.columns.str.startswith('wt_value')].copy()
    
    hr=pd.melt(hr, id_vars=['individual_id','startdate','enddate','result_stream'], 
                  value_vars=hr.columns[hr.columns.str.contains('weight')]).copy()
    
    hr=hr[hr.value!=0]
    hr['final_source']=hr['variable'].astype(str)+':' +hr['value'].astype(str)
    hr.drop(columns=['variable','value'],inplace=True)
    hr=hr.groupby(['individual_id','startdate','enddate','result_stream'])['final_source'].apply(';'.join).reset_index()
    
    # display(hr.head(6))
    # print("hr dim:", hr.shape)
    data=hr[['individual_id','startdate','enddate','final_source','result_stream']].copy()
    data.rename(columns={'result_stream':'value','final_source':'Personicle'},inplace=True)
    data['Personicle'] = data['Personicle'].str.replace('weight_','')
    #data=hr.to_dict('index')
    data=data.to_json(orient='records')
    total_data_points=len(data)
    # print(total_data_points)
    
    try:
        datastream_producer(data)
    except Exception as e:
    
        #logging.info("Total data points added for source {}: {}".format(datasource, total_data_points))
        logging.info("Total data points added for source {}: {}".format(data, total_data_points))
        logging.error(traceback.format_exc())
           
    return data

if __name__ == '__main__':
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    print("running server on {}:{}".format(DATA_SYNC_CONFIG['HOST_URL'], DATA_SYNC_CONFIG['HOST_PORT']))
    app.run(DATA_SYNC_CONFIG['HOST_URL'], port=DATA_SYNC_CONFIG['HOST_PORT'], debug=True)#, ssl_context='adhoc')



