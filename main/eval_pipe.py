import pandas as pd
import datetime
from hdbcli import dbapi
from dateutil.relativedelta import relativedelta
from utils.tools import acc_caculate
from models.model_performance import fcst_performance

class EvaluationPIPE:
    def __init__(self, total_result, product_info, ZSTRATEGY_data):
        self.fcst_result = pd.concat(total_result)
        self.product_info = product_info
        self.fcst_result_aiflag = self.fcst_result.merge(product_info, on='PART_NO', how='left')
        self.ZSTRATEGY_data = ZSTRATEGY_data
        
    def process_c_category(self):
        self.fcst_result_aiflag['Pred_demand'] = self.fcst_result_aiflag.apply(
            lambda row: 0 if row['ABC_INDICATOR'].startswith('C') and row['Pred_demand'] < 5 else row['Pred_demand'], axis=1)
        # return self.fcst_result_aiflag
    
    def merge_actual_qty( self, gloab_demand_pivot):
        merged_df= self.fcst_result_aiflag.merge(gloab_demand_pivot, on='PART_NO', how='left')
        self.fcst_result_aiflag= merged_df

    def acc_caculation(self):
        qty_types = ['Pred_demand', 'MSU_QTY', 'Model_QTY', 'DS_QTY', 'AWS_QTY']  # 可以根據需要添加更多的 QTY 類型
        for qty_type in qty_types:
            acc_column_name = f'{qty_type.split("_")[0]}_Acc'
            self.fcst_result_aiflag[acc_column_name] = acc_caculate(
                self.fcst_result_aiflag['ORDER_QTY'],
                self.fcst_result_aiflag[qty_type]
            )
                
    def evaluate_performance(self, data):
        evaluate_result=fcst_performance(data)
        return evaluate_result
    
    def generate_commit_format(self, fcst_month):
        today = datetime.datetime.today()
        commit_data = pd.DataFrame()
        commit_data['MATNR'] = self.fcst_result_aiflag.loc[:, "PART_NO"]
        commit_data['MANDT'] = "168"
        commit_data['WERKS'] = ''
        commit_data['FCST_TYPE'] = 'D'
        commit_data['FCST_DATE'] = (today + relativedelta(months=fcst_month)).strftime('%Y%m')
        commit_data['CREATE_DATE'] = today.strftime('%Y%m%d')
        commit_data['FCST_QTY'] = self.fcst_result_aiflag.loc[:, "Pred_demand"]
        commit_data['AIFCST_RATE'] = "0"
        commit_data['MOVING_AVG_QTY'] = '0'
        commit_data['MOVING_AVG_RATE'] = '0'
        commit_data["EVALUTE_FLAG"] = '0'
        commit_data["ALERT_FLAG"] = '0'
        commit_data = commit_data.merge(self.ZSTRATEGY_data[['MATNR','ZSTRATEGY']], on='MATNR', how='left')
        commit_data['ZSTRATEGY'] = commit_data['ZSTRATEGY'].fillna('')
        commit_data['AI_TREND'] = self.fcst_result_aiflag.loc[:, "labels"]

        # 根據 fcst_month 判斷是否執行 SA 和 S9 工作
        if fcst_month <= 2:
            commit_data.loc[commit_data['ZSTRATEGY'] == 'SA', 'FCST_QTY'] *= 1.1
            commit_data.loc[commit_data['ZSTRATEGY'] == 'S9', 'FCST_QTY'] *= 0.8
        commit_data['FCST_QTY'] = commit_data['FCST_QTY'].round().astype(int)

        return commit_data[['MANDT', 'MATNR', 'WERKS', 'FCST_TYPE', 'FCST_DATE', 'CREATE_DATE',
       'FCST_QTY', 'AIFCST_RATE', 'MOVING_AVG_QTY', 'MOVING_AVG_RATE',
       'EVALUTE_FLAG', 'ALERT_FLAG', 'ZSTRATEGY', 'AI_TREND']]
        
