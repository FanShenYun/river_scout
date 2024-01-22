import requests
import pandas as pd
import json
from pandas import json_normalize
from datetime import datetime, timedelta
from linebot import LineBotApi
from linebot.models import TextSendMessage
import configparser

# KEYS
path = '/home/shenyun/python/config.cfg'
config = configparser.ConfigParser()
config.read(path)
key_info = config["API_KEY"]
API_KEY = key_info['rainfall']
LINE_BOT = key_info['LINE_BOT']
# rainfall data website
# rainfall data document: https://opendata.cwa.gov.tw/opendatadoc/Observation/O-A0002-001.pdf
dest_url = f'https://opendata.cwa.gov.tw/fileapi/v1/opendataapi/O-A0002-001?Authorization={API_KEY}&downloadType=WEB&format=JSON'

# rain_guage & water_sport destinations 
df_rain = pd.read_csv('/home/shenyun/python/rain_guage.csv',encoding='utf-8')
df_water = pd.read_csv('/home/shenyun/python/water_sport.csv',encoding='utf-8')

# get rainfall data
r = requests.get(dest_url)
info = json.loads(r.text)
# rainfall data cleaning
df = json_normalize(info['cwaopendata']['dataset']['Station'])
df_columns = ['StationName', 'StationId','ObsTime.DateTime','RainfallElement.Past10Min.Precipitation','RainfallElement.Past1hr.Precipitation']
df = df[df_columns]
df['RainfallElement.Past10Min.Precipitation'] = df['RainfallElement.Past10Min.Precipitation'].astype(float)
df['RainfallElement.Past1hr.Precipitation'] = df['RainfallElement.Past1hr.Precipitation'].astype(float)

# send a notification when the rainfall exceeds 10mm within a 10-minute interval
temp_10min = []
for i in df['StationId'].loc[df['RainfallElement.Past10Min.Precipitation']>=10.0]:
    j = df_rain['Catchment'].loc[df_rain['StationId']==i]
    if j.empty == True:
        continue
    else:
        j = j.to_string(index=False)
    k = df_water['Name'].loc[df_water['Catchment']==j]
    StationName = df_rain['StationNam'].loc[df_rain['StationId']==i].to_string(index=False)
    Past10Min = float(df['RainfallElement.Past10Min.Precipitation'].loc[df['StationId']==i].iloc[0])
    for l in k:
        Name = l
        COUNTYNAME = df_water['COUNTYNAME'].loc[df_water['Name']==l].to_list()
        COUNTYNAME = ''.join(set(COUNTYNAME))
        alert = f'{StationName}雨量站10分鐘降雨量達{Past10Min}mm請位於{COUNTYNAME}{l}進行水域活動的民眾注意溪水狀況'
        temp_10min.append(alert)
temp_10min = list(set(temp_10min))

# push message
line_bot_api = LineBotApi(LINE_BOT) 
user = pd.read_csv('/home/shenyun/python/user_list.csv',encoding='utf-8')
group = pd.read_csv('/home/shenyun/python/group_list.csv',encoding='utf-8')
for i in temp_10min:
    # to single user
    for user_id in user['UserId']:
        line_bot_api.push_message(user_id,TextSendMessage(text=f'{i}'))
    # to group user
    for group_id in group['GroupId']:
        line_bot_api.push_message(group_id,TextSendMessage(text=f'{i}'))
