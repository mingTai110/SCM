import pandas as pd
from dateutil.relativedelta import relativedelta
from  utils.tools import Caculate_Gap_year

so_ori=pd.read_csv('Dataset\So_Data.csv')
product_ori=pd.read_csv('Dataset\Product_Data.csv')
open_so_ori=pd.read_csv('Dataset\open_so_ori.csv')

so_list=['PART_NO','SHIP_PLANT', 'REQUIRED_DATE', 'COMPANY_ID', 'ORDER_QTY']
product_list= ['PART_NO','PG', 'PD', 'MODEL', 'ABC_INDICATOR']

def SO_Data_preprocess():
    SO_info=so_ori.merge(product_ori, on='PART_NO', how='inner')
    SO_info['year_month']=pd.to_datetime(SO_info.REQUIRED_DATE).dt.to_period('M').dt.to_timestamp() # obj->datetime64[ns]->月頻 -> 日頻
    # SO_info=SO_info[['COMPANY_ID','SHIP_PLANT','PD','MODEL','PART_NO','year_month','REQUIRED_DATE','ORDER_QTY']]
    return SO_info

def Openso_Preprocess(data=open_so_ori, version_year=None):
    #計算gap_year
    data['Gap_Year']=Caculate_Gap_year(data.VERSION, data.YM)
    #選擇週期
    data=data[data['Gap_Year']==version_year]
    OP_info=data.merge(product_ori, on='PART_NO', how='inner')
    OP_info['year_month'] = pd.to_datetime(OP_info['YM'], format='%Y/%m/%d')
    OP_info=OP_info[['COMPANY_ID', 'year_month', 'PG', 'PD', 'MODEL','PART_NO', 'SHIP_PLANT', 'OPEN_QTY', 'ABC_INDICATOR']]

    return OP_info

def Upload_Data(new_data,Region_class,today):
    upload_columns=['MANDT','MATNR','WERKS','FCST_TYPE','FCST_DATE','CREATE_DATE','FCST_QTY','AIFCST_RATE','MOVING_AVG_QTY','MOVING_AVG_RATE','EVALUTE_FLAG','ALERT_FLAG','ZSTRATEGY','AI_TREND']
    new_data['MANDT']='168'
    new_data['FCST_TYPE']='R'
    new_data['CREATE_DATE']=today.strftime('%Y%m%d')
    new_data['FCST_DATE']=(today+relativedelta(months=2)).strftime('%Y%m')
    new_data['MOVING_AVG_QTY']='0'
    new_data['MOVING_AVG_RATE']='0'
    new_data["EVALUTE_FLAG"]='0'
    new_data["ALERT_FLAG"]='0'
    new_data["FCST_QTY"]=0
    new_data['AIFCST_RATE']=0
    new_data['WERKS']=Region_class+"H1"
    new_data["ZSTRATEGY"]=''

    new_data= new_data[upload_columns]
    return new_data