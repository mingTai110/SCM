import pandas as pd

def backtesting(days, PD_Material_list, fcst_month, start_date, end_date, full_date_range, benchmark_processor):

    ai_fcst_data=benchmark_processor.MSU_data_preprocess(start_date, end_date, gap_year=fcst_month, IIOT=True)
    monthly_msu_fcst=[]
    monthly_ds_fcst=[]
    monthly_aws_fcst=[]
    monthly_model_fcst=[]
    backtest_data=pd.DataFrame()
    for PART_NO_name in PD_Material_list:
        monthly_fcst=benchmark_processor.process( ai_fcst_data, PART_NO_name, days, full_date_range, fcst_month=fcst_month)
        monthly_msu_fcst.append(monthly_fcst['MSU_QTY'])
        monthly_ds_fcst.append(monthly_fcst['DS_QTY'])
        monthly_aws_fcst.append(monthly_fcst['AWS_QTY'])
        monthly_model_fcst.append(monthly_fcst['MODEL_QTY'])
        
    backtest_data['PART_NO']=PD_Material_list
    backtest_data['MSU_QTY']=pd.Series(monthly_msu_fcst)
    backtest_data['Model_QTY']=pd.Series(monthly_model_fcst)
    backtest_data['DS_QTY']=pd.Series(monthly_ds_fcst)
    backtest_data['AWS_QTY']=pd.Series(monthly_aws_fcst)
    return backtest_data