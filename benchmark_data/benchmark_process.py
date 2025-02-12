import pandas as pd
import numpy as np
from utils.tools import Caculate_Gap_year,fiilna

from benchmark_data.non_posprocesing_data import orgin_fcst_result

class BenchmarkProcessor:
    def __init__(self):
        self.aifcst_postprocess = None
        self.aifcst_nonpostprocess = None
    def load_data(self, file_path):
        # Load data from the specified file path
        self.aifcst_postprocess= pd.read_csv(file_path)
        self.aifcst_nonpostprocess=orgin_fcst_result(start_month_version=2, evaluation_months=8)

    def MSU_data_preprocess(self, start_date, end_date, gap_year=2, IIOT=True):
        if self.aifcst_postprocess is None:
            raise ValueError("Data not loaded. Please load the data using the load_data method.")
        
        selected_data = self.aifcst_postprocess[['MATNR', 'FCST_TYPE', 'CREATE_DATE', 'FCST_DATE', 'FCST_QTY']].copy()
        
        # Convert date columns to string if not already
        if selected_data['CREATE_DATE'].dtype != 'object': 
            selected_data['CREATE_DATE'] = selected_data['CREATE_DATE'].astype(str)
            selected_data['FCST_DATE'] = selected_data['FCST_DATE'].astype(str)
        
        # Filter based on IIOT condition
        if IIOT:
            selected_data = selected_data[selected_data['CREATE_DATE'].str[-2:].astype(int) < 22]
        else:
            selected_data = selected_data[selected_data['CREATE_DATE'].str[-2:].astype(int) < 15]

        # Calculate gap year
        selected_data['Gap_Year'] = Caculate_Gap_year(selected_data.CREATE_DATE, selected_data.FCST_DATE)
        selected_data['year_month'] = pd.to_datetime(selected_data['FCST_DATE'], format='%Y%m')

        # Apply query conditions
        month_gap_condition = "Gap_Year == @gap_year"
        fcst_type_condition = "FCST_TYPE in ['D', 'M', 'A']"
        date_condition = "year_month >= @start_date and year_month <= @end_date"
        query_conditions = f"{month_gap_condition} and {fcst_type_condition} and {date_condition}"
        selected_data = selected_data.query(query_conditions).reset_index(drop=True)
        
        # Rename columns
        selected_data.rename(columns={'MATNR': 'PART_NO'}, inplace=True)
        return selected_data
    

    def fsct_type(self, data, type):
        type_data = data.query('FCST_TYPE == @type')
        type_data = type_data.groupby(['PART_NO', 'year_month']).agg({'FCST_QTY': 'last'}).reset_index()
        return type_data

    def get_comparision_data(self, part_data, types=['M','D','A']):
        
        msu_fcst =self.fsct_type(part_data, types[0])
        ds_fcst  =self.fsct_type(part_data, types[1])
        aws_fcst =self.fsct_type(part_data, types[2])
        merge_df =msu_fcst.merge(ds_fcst,on=['year_month','PART_NO'],how='left')
        merge_df =merge_df.merge(aws_fcst,on=['year_month','PART_NO'],how='left')
        merge_df.columns=['PART_NO','year_month', 'MSU_QTY', 'DS_QTY','AWS_QTY']
        return merge_df
    
    def fcst_demand(self, data, days, fcst_month=2,columns_name='MSU_QTY'):
        result = data.loc[days + fcst_month, columns_name]
        return result
    
    def process(self, ai_fcst_source, PART_NO_name, days, full_date_range , fcst_month=2):

        part_data= ai_fcst_source.query('PART_NO==@PART_NO_name')
        nonpostprocess_data= self.aifcst_nonpostprocess.query('PART_NO==@PART_NO_name')
        merge_df = self.get_comparision_data(part_data, types=['M', 'D', 'A'])
        merge_df = fiilna(merge_df, full_date_range, QTY_name='MSU_QTY')
        merge_df = fiilna(merge_df, full_date_range, QTY_name='AWS_QTY')
        merge_df['DS_QTY'] = merge_df['DS_QTY'].fillna(merge_df['MSU_QTY'])
        merge_df['MODEL_QTY']= fiilna(nonpostprocess_data, full_date_range , QTY_name= 'MODEL_QTY' ).loc[:,'MODEL_QTY']
        monthly_fcst= self.fcst_demand(merge_df, days, fcst_month, columns_name=['MSU_QTY','DS_QTY','AWS_QTY','MODEL_QTY'])
        return monthly_fcst
