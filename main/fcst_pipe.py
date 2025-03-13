from input_preprocess.indicator_processor import IndicatorProcessor
from models.feature_engineering import FeatureProcessor
from models.feature_processing import train_valid_split
from models.forecast_process import ModelProcessor
from utils.tools import Plot_History_Trend

n_features = 49 # 假設要生成 10 個 feature
excluded_features = {4,5,6,14,15,23,24,29,30,31,32,33,34,35,36,37,38,44,45,46}
feature_list = [f"Feature_{i}" for i in range(1, n_features + 1) if i not in excluded_features]
input_columns=['labels']+feature_list
output_columns=['Output']

def MSU_FCST(days, PD_Material_list, fcst_month, full_date_range, Used_Data, filtered_open_data_1, filtered_open_data_2, filtered_open_data_3,filtered_open_data_4, filtered_open_data_5, plot=True):
    train_input_list=[]
    train_output_list=[]
    valid_input_list=[]
    valid_output_list=[]
    for PART_NO_name in PD_Material_list:

        indicator_processor = IndicatorProcessor(full_date_range)
        PART_NO_SO, PART_NO_Open_1, PART_NO_Open_2, PART_NO_Open_3, Trend_Indicator, Evaluate_Table = indicator_processor.generate_indicators(PART_NO_name, Used_Data, filtered_open_data_1, filtered_open_data_2, filtered_open_data_3)
        
        date_column = 'year_month'
        PART_NO_Open_4= indicator_processor.process_and_fillna(filtered_open_data_4, PART_NO_name, date_column, 'OPEN_QTY')
        PART_NO_Open_5= indicator_processor.process_and_fillna(filtered_open_data_5, PART_NO_name, date_column, 'OPEN_QTY')

        feature_processor = FeatureProcessor()
        input_data, output_data = feature_processor.feature_process(PART_NO_Open_1, PART_NO_Open_2, PART_NO_Open_3, PART_NO_Open_4, PART_NO_Open_5, PART_NO_SO, Trend_Indicator, input_columns, output_columns, y_idx=fcst_month)        
        train_input, test_input, train_output, test_output= train_valid_split(input_data, output_data, days , fcst_month, fcst_days=1, train_month= 12)#-4
     
        train_input_list.append(train_input)
        valid_input_list.append(test_input)
        train_output_list.append(train_output)
        valid_output_list.append(test_output)

        if plot and days==25:
            Plot_History_Trend(PART_NO_name, PART_NO_Open_1, PART_NO_Open_2, PART_NO_Open_3, PART_NO_SO, Trend_Indicator )
        
    model_processor = ModelProcessor()
    fcst_data = model_processor.process_model(train_input_list, train_output_list, valid_input_list, valid_output_list, model_name='random_forest', plot=False)
    fcst_data['PART_NO']=PD_Material_list
    return fcst_data