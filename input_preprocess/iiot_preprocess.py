import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.tools import Caculate_Gap_year
    
class DataPreprocessor:
    def __init__(self, product_data, so_data, wo_data, open_wo_data, open_so_data):
        self.product_ori = product_data
        self.so_data = so_data
        self.wo_data = wo_data
        self.open_wo_data = open_wo_data
        self.open_so_data = open_so_data

    def so_data_preprocess(self, date_columns="REQUIRED_DATE"):
        # Merge SO data with product information and convert date columns to year-month format
        so_info = self.so_data.merge(self.product_ori, on='PART_NO', how='inner')
        so_info['year_month'] = pd.to_datetime(so_info[date_columns]).dt.to_period('M').dt.to_timestamp()
        return so_info

    def wo_data_preprocess(self, date_columns="START_DATE"):
        # Merge WO data with product information and convert date columns to year-month format
        wo_data = self.wo_data[['PART_NO', 'SHIP_PLANT', 'CREATE_DATE', 'START_DATE', 'SHIP_DATE', 'QTY']].copy()
        wo_data = wo_data.merge(self.product_ori, on='PART_NO', how='inner')
        wo_data['year_month'] = pd.to_datetime(wo_data[date_columns]).dt.to_period('M').dt.to_timestamp()
        return wo_data.rename(columns={"QTY": "ORDER_QTY"})

    def so_wo_combination(self, so_data, wo_data):
        # Combine SO and WO data
        so_temp = so_data[['year_month', 'PG', 'PD', 'MODEL', 'PART_NO', 'ABC_INDICATOR', 'ORDER_QTY']]
        wo_temp = wo_data[['year_month', 'PG', 'PD', 'MODEL', 'PART_NO', 'ABC_INDICATOR', 'ORDER_QTY']]
        merged_df = pd.concat([so_temp, wo_temp], ignore_index=True)
        return merged_df

    def openso_preprocess(self, data, version_year=None):
        # Preprocess open SO data
        data['Gap_Year'] = Caculate_Gap_year(data.VERSION, data.YM)
        data = data[data['Gap_Year'] == version_year]
        op_info = data.merge(self.product_ori, on='PART_NO', how='inner')
        op_info['year_month'] = pd.to_datetime(op_info['YM'], format='%Y/%m/%d')
        return op_info

    def merge_open_so_wo(self, gap_year=1):
        # Merge open SO and WO data
        temp_openwo = self.openso_preprocess(data=self.open_wo_data, version_year=gap_year)[['year_month', 'PG', 'PD', 'MODEL', 'PART_NO', 'OPEN_QTY', 'ABC_INDICATOR']]
        temp_openso = self.openso_preprocess(data=self.open_so_data, version_year=gap_year)[['year_month', 'PG', 'PD', 'MODEL', 'PART_NO', 'SHIP_PLANT', 'OPEN_QTY', 'ABC_INDICATOR']]
        merged_df = pd.concat([temp_openwo, temp_openso])
        return merged_df

    def filter_data(self, data, MSU, ABC_class, PG_class, start_date, end_date, Region_class=None):
        # Define filter conditions
        ABC_condition = data['ABC_INDICATOR'].str.startswith(ABC_class)
        PG_condition = data['PG'].isin(PG_class)
        date_condition = (data['year_month'] >= start_date) & (data['year_month'] <= end_date)
        
        if MSU:
            filtered_data = data[ABC_condition & PG_condition & date_condition]
        else:
            Region_condition = data['SHIP_PLANT'].str.startswith(Region_class)
            filtered_data = data[ABC_condition & PG_condition & date_condition & Region_condition]
        
        return filtered_data.reset_index(drop=True)
    

    def filter_and_aggregate(self, data, target_year_month):

        filtered_data = data[data['year_month'] == target_year_month]
        filtered_data = filtered_data.rename(columns={'PART_NO': 'MATNR'})
        result = filtered_data.groupby(['MATNR', 'year_month'], as_index=False).agg({
            'OPEN_QTY': 'sum',
            'PG': 'first',
            'PD': 'first',
            'MODEL': 'first',
            'ABC_INDICATOR': 'first',
            'SHIP_PLANT': 'first'
        })
        return result

    def order_qty_avg_6m(self, collect_so_wo_histo, today):
        result_gloab_demand= collect_so_wo_histo.groupby(['PART_NO', 'year_month'], as_index=False).agg({
            'ORDER_QTY': 'sum',
            'PG': 'first',
            'PD': 'first',
            'MODEL': 'first',
            'ABC_INDICATOR': 'first',})

        result_gloab_demand['ORDER_QTY_AVG_6M'] = result_gloab_demand.groupby('PART_NO')['ORDER_QTY'].transform(lambda x: x.rolling(window=6, min_periods=1).mean())
        result_gloab_demand['ORDER_QTY_AVG_6M'] = result_gloab_demand['ORDER_QTY_AVG_6M'].round().astype(int)
        result_gloab_demand=result_gloab_demand[result_gloab_demand['year_month']==(today+relativedelta(months=-1)).strftime('%Y-%m-01')]
        result_gloab_demand = result_gloab_demand.rename(columns={'PART_NO': 'MATNR'})
        return result_gloab_demand 