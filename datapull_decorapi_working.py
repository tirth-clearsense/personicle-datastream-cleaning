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
from flask import Flask, Response,jsonify
from flask import request
import requests

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

from config import PERSONICLE_SCHEMA_API

app = Flask(__name__)

def find_personicle_datastream(personicle_data_type):
    stream_information = requests.get(PERSONICLE_SCHEMA_API['MATCH_DICTIONARY_ENDPOINT'], params={
            "data_type": "datastream",
            "stream_name": personicle_data_type
        }, verify=False)
        
    # personicle_data_description = find_datastream(personicle_data_type)
    if stream_information.status_code != requests.codes.ok:
        logging.warn("Data type {} not present in personicle data dictionary".format(personicle_data_type))
        logging.warn(stream_information.text)
        return None
    logging.info(stream_information.text)
    personicle_data_description = json.loads(stream_information.text)
    return personicle_data_description

def validate_personicle_data_packet(data_packet):
    print("Validating data packet: {}".format(data_packet))
    stream_information = requests.post(PERSONICLE_SCHEMA_API['SCHEMA_VALIDATION_ENDPOINT'], params={
            "data_type": "datastream"
        }, json=data_packet, verify=False)
    print(stream_information.content)
    validation_response = stream_information.json()
    return validation_response['schema_check']

@app.route("/")
def test_application():
    return Response("Data cleaning service up")

@app.route('/fetch_data/',methods=['GET'])
def request_page():
    
    user  = request.args.get('user',type=str , default='')
    data_type  = request.args.get('data', type=str ,default='')
    source  = request.args.get('source',type=str , default='')
    sources = source.split(";") if source is not None else None
    #print("sources:",sources)
    ts  = request.args.get('starttime',type=str , default='')
    te  = request.args.get('endtime',type=str , default='')
    freq  = request.args.get('freq',type=str , default='1min')

    personicle_stream_info = find_personicle_datastream(data_type)
    tblname = personicle_stream_info['TableName']

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
    
    
    print("hr dim:", hr.shape)
    data=hr[['individual_id','startdate','enddate','final_source','result_stream']].copy()
    data.rename(columns={'result_stream':'value','final_source':'Personicle'},inplace=True)
    if personicle_stream_info['ValueType'] == 'Integer':
        data['value'] = data['value'].astype(int)


    data['Personicle'] = data['Personicle'].str.replace('weight_','')
    #data=hr.to_dict('index')
    data['timestamp'] = data['enddate']
    
    # format the data packet, validate the data packet, then send to producer
    data_packet = {
        "streamName": data_type,
        "individual_id": data['individual_id'].iloc[0],
        "source": "Personicle",
        "unit": personicle_stream_info['Unit'],
        "dataPoints": json.loads(data[["timestamp", "value"]].to_json(orient='records'))
    }

    validation_response = validate_personicle_data_packet(data_packet)
    if not validation_response:
        return jsonify({'message': 'incorrectly formatted data packet'})

    data=data.to_json(orient='records')
    total_data_points=len(data)
    print(total_data_points)
   
    try:
        datastream_producer(data_packet)
        return jsonify({
            'message': 'Successfully sent data packet', 
            'request':{
                'dataType': data_type,
                'sources': source,
                'frequency': freq,
                'start_Time': ts,
                'end_time': te,
                'datapoints': total_data_points
            }
            
        })
    except Exception as e:    
        #logging.info("Total data points added for source {}: {}".format(datasource, total_data_points))
        logging.info("Total data points added for source {}: {}".format(data, total_data_points))
        logging.error(traceback.format_exc())
        return jsonify({'message': 'Error while sending data packet; {}'.format(e)})

if __name__ == '__main__':
     app.run(port=7777, debug=True)



