import pandas as pd
from main.data_filter_pipe import process_and_filter_data 
from main.fcst_pipe import MSU_FCST
from main.backtest_pipe import backtesting
from main.eval_pipe import EvaluationPIPE

class ForecastPIPE:
    def __init__(self, preprocessor, start_date, end_date, fcst_month, product_info, ZSTRATEGY_data):
        self.preprocessor = preprocessor
        self.start_date = start_date
        self.end_date = end_date
        self.fcst_month = fcst_month
        self.product_info = product_info
        self.ZSTRATEGY_data = ZSTRATEGY_data
        self.full_date_range = pd.date_range(start=start_date, end=end_date, freq='MS').normalize()
    
    def prepare_data(self, ABC_class, so_data, open_data_list, MSU=True, PG_class=['ISG', 'IAG', 'ICWG', 'ESG']):
        """處理數據，返回已過濾的資料與開放數據"""
        print("ABC類:", ABC_class)
        filtered_data, filtered_open_data = process_and_filter_data(
            self.preprocessor, ABC_class, MSU, PG_class, self.start_date, self.end_date, so_data, open_data_list
        )
        #　filtered_open_data = {1: open_data_1, 2: open_data_2, 3: open_data_3, 4: open_data_4, 5: open_data_5}
        filtered_so_data = filtered_data[['year_month', 'PD', 'MODEL', 'PART_NO', 'ORDER_QTY']]
        PD_Material = (filtered_so_data.groupby(['PART_NO'])['ORDER_QTY'].sum()).reset_index()
        return PD_Material, filtered_so_data, filtered_open_data

    def generate_forecast(self, days, PD_Material, filtered_so_data, filtered_open_data):
        """產生預測數據"""

        return MSU_FCST(days, PD_Material.PART_NO.unique(), self.fcst_month, self.full_date_range, filtered_so_data, filtered_open_data[1], filtered_open_data[2],filtered_open_data[3],filtered_open_data[4],filtered_open_data[5],plot=False)
                
    def run_backtest(self, days, PD_Material, benchmark_processor):
        """執行回測"""
        return backtesting(days, PD_Material.PART_NO.unique(), self.fcst_month, self.start_date, self.end_date, self.full_date_range, benchmark_processor)

    def evaluate_data(self, total_result, days, gloab_demand_pivot):
        """評估預測結果"""
        eval_processor = EvaluationPIPE(total_result, self.product_info, self.ZSTRATEGY_data)
        eval_processor.process_c_category()
        
        actual_data = gloab_demand_pivot.iloc[:, [0, days + 1 + self.fcst_month]]
        actual_data = actual_data.rename(columns={actual_data.columns[1]: 'ORDER_QTY'})
        
        eval_processor.merge_actual_qty(actual_data)
        eval_processor.acc_caculation()
        
        return eval_processor.fcst_result_aiflag, eval_processor

    def run_validation(self, so_data, open_data_list, benchmark_processor, gloab_demand_pivot):
        """執行完整的預測驗證流程"""
        total_result = []

        for ABC_class in ['A', 'B', 'C']:
            PD_Material, filtered_so_data, filtered_open_data = self.prepare_data(ABC_class, so_data, open_data_list)
            
            for days in range(25, 26):
                fcst_data = self.generate_forecast(days, PD_Material, filtered_so_data, filtered_open_data) # plot parameter
                backtest_data = self.run_backtest(days, PD_Material, benchmark_processor)
                concate_result = pd.merge(fcst_data, backtest_data, on='PART_NO', how='left')
                total_result.append(concate_result)
        
        return self.evaluate_data(total_result, days, gloab_demand_pivot)
    
    def run_inference(self, so_data, open_data_list):
        """執行完整的預測流程"""
        total_result = []

        for ABC_class in ['A', 'B', 'C']:
            PD_Material, filtered_so_data, filtered_open_data = self.prepare_data(ABC_class, so_data, open_data_list)
            
            for days in range(25, 26):
                fcst_data = self.generate_forecast(days, PD_Material, filtered_so_data, filtered_open_data)
                concate_result = fcst_data
                total_result.append(concate_result)

        eval_processor = EvaluationPIPE(total_result, self.product_info, self.ZSTRATEGY_data)
        eval_processor.process_c_category()
        commit_data = eval_processor.generate_commit_format(self.fcst_month)
        return commit_data
