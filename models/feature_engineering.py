import pandas as pd
import numpy as np

def train_valid_split(input_data, output_data, end_date, fcst_month, fcst_days=1,train_month= 13):
    
    train_input=pd.DataFrame(input_data[end_date-train_month-fcst_month:end_date-fcst_month ]).iloc[:,1:].reset_index(drop=True)
    train_output=pd.DataFrame(output_data[end_date-train_month-fcst_month:end_date-fcst_month ]).reset_index(drop=True)
    train_output['labels']=pd.DataFrame(input_data[end_date-train_month-fcst_month:end_date-fcst_month]).iloc[:,0].reset_index(drop=True)

    test_input=pd.DataFrame(input_data[end_date: end_date+fcst_days]).iloc[:,1:].reset_index(drop=True)
    test_output=pd.DataFrame(output_data[end_date: end_date+fcst_days]).reset_index(drop=True)
    test_output['labels']=pd.DataFrame(input_data[end_date: end_date+fcst_days]).iloc[:,0].reset_index(drop=True)
    
    return train_input, test_input, train_output, test_output

class FeatureProcessor:
    def __init__(self):
        pass

    def feature_data(self, PART_NO_Openso_1, PART_NO_Openso_2, PART_NO_Openso_3, PART_NO_Openso_4 ,PART_NO_Openso_5, PART_NO_SO, Trend_Indicator):
        data = pd.DataFrame()
        data['Date'] = PART_NO_Openso_1.year_month
        data['ORDER_QTY'] = PART_NO_SO.ORDER_QTY
        data['Openso_1'] = PART_NO_Openso_1.OPEN_QTY
        data['Openso_2'] = PART_NO_Openso_2.OPEN_QTY
        data['Openso_3'] = PART_NO_Openso_3.OPEN_QTY
        # 3 to 2 
        Openso3and2_diff = PART_NO_Openso_2.OPEN_QTY - PART_NO_Openso_3.OPEN_QTY
        data['Feature_1'] = Openso3and2_diff.shift(-1).fillna(0)
        data['Feature_2'] = Openso3and2_diff.shift(0).fillna(0)
        data['Feature_3'] = Openso3and2_diff.shift(1).fillna(0)
        data['Feature_4'] = Openso3and2_diff.shift(2).fillna(0)
        data['Feature_5'] = Openso3and2_diff.shift(3).fillna(0)
        data['Feature_6'] = Openso3and2_diff.shift(4).fillna(0)

        data['Feature_7']= Openso3and2_diff.rolling(window=3).mean().shift(-1).fillna(0)
        data['Feature_8']= Openso3and2_diff.rolling(window=3).std().shift(-1).fillna(0)
        data['Feature_9']= Openso3and2_diff.rolling(window=6).mean().shift(-1).fillna(0)
        data['Feature_10']= Openso3and2_diff.rolling(window=6).std().shift(-1).fillna(0)
        
        # 2 to 1
        Openso2and1_diff = PART_NO_Openso_1.OPEN_QTY - PART_NO_Openso_2.OPEN_QTY
        data['Feature_11']=  Openso2and1_diff.shift(0).fillna(0)
        data['Feature_12'] = Openso2and1_diff.shift(1).fillna(0)
        data['Feature_13'] = Openso2and1_diff.shift(2).fillna(0)
        data['Feature_14'] = Openso2and1_diff.shift(3).fillna(0)
        data['Feature_15'] = Openso2and1_diff.shift(4).fillna(0)

        data['Feature_16']= Openso2and1_diff.rolling(window=3).mean().shift(0).fillna(0)
        data['Feature_17']= Openso2and1_diff.rolling(window=3).std().shift(0).fillna(0)
        data['Feature_18']= Openso2and1_diff.rolling(window=6).mean().shift(0).fillna(0)
        data['Feature_19']= Openso2and1_diff.rolling(window=6).std().shift(0).fillna(0) 
       
        # 1 to so 
        Openso1andso_diff = PART_NO_SO.ORDER_QTY - PART_NO_Openso_1.OPEN_QTY
        data['Feature_20']= Openso1andso_diff.shift(1).fillna(0)
        data['Feature_21']= Openso1andso_diff.shift(2).fillna(0)
        data['Feature_22']= Openso1andso_diff.shift(3).fillna(0)
        data['Feature_23']= Openso1andso_diff.shift(4).fillna(0)
        data['Feature_24']= Openso1andso_diff.shift(5).fillna(0)

        data['Feature_25']= Openso1andso_diff.rolling(window=3).mean().shift(1).fillna(0)
        data['Feature_26']= Openso1andso_diff.rolling(window=3).std().shift(1).fillna(0)
        data['Feature_27']= Openso1andso_diff.rolling(window=6).mean().shift(1).fillna(0)
        data['Feature_28']= Openso1andso_diff.rolling(window=6).std().shift(1).fillna(0)

        # 近三個月的open and historcal dat mean and std
        data['Feature_29'] = data['Openso_1'].rolling(window=3).mean().shift(0).fillna(0)
        data['Feature_30'] = data['Openso_1'].rolling(window=3).std().shift(0).fillna(0)
        data['Feature_31'] = data['Openso_2'].rolling(window=3).mean().shift(-1).fillna(0)
        data['Feature_32'] = data['Openso_2'].rolling(window=3).std().shift(-1).fillna(0)
        data['Feature_33'] = data['Openso_3'].rolling(window=3).mean().shift(-2).fillna(0)
        data['Feature_34'] = data['Openso_3'].rolling(window=3).std().shift(-2).fillna(0)
        data['Feature_35']=  data['ORDER_QTY'].rolling(window=3).mean().shift(1).fillna(0)
        data['Feature_36'] = data['ORDER_QTY'].rolling(window=3).std().shift(1).fillna(0)
        data['Feature_37']=  data['ORDER_QTY'].rolling(window=6).mean().shift(1).fillna(0)
        data['Feature_38'] = data['ORDER_QTY'].rolling(window=6).std().shift(1).fillna(0)
        
        data['Feature_39'] = data['Openso_3'].shift(-2).fillna(0) - data['Openso_3'].shift(-1).fillna(0)
        data['Feature_40'] = data['Openso_3']
        data['Feature_41'] = data['Openso_3'].shift(-2).fillna(0)
        data['Feature_42'] = data['Openso_2'].shift(-1).fillna(0)
        data['Feature_43'] = data['Openso_2'].shift(0).fillna(0)

        data['Feature_44'] = data['Openso_1'].rolling(window=6).mean().shift(0).fillna(0)
        data['Feature_45'] = data['Openso_2'].rolling(window=6).mean().shift(-1).fillna(0)
        data['Feature_46'] = data['Openso_3'].rolling(window=6).mean().shift(-2).fillna(0)
        #近三個月實際需求量 
        data['Feature_47']=data['ORDER_QTY'].shift(1).fillna(0)
        data['Feature_48']=data['ORDER_QTY'].shift(2).fillna(0)
        data['Feature_49']=data['ORDER_QTY'].shift(3).fillna(0)
        # 3個月前訂單到2個月前訂單增減量
        Openso3and4_diff = PART_NO_Openso_3.OPEN_QTY - PART_NO_Openso_4.OPEN_QTY
        data['Feature_50']= Openso3and4_diff.shift(-2).fillna(0)
        data['Feature_51']= Openso3and4_diff.shift(-1).fillna(0)
        data['Feature_52']= Openso3and4_diff.shift(0).fillna(0)
        # 4個月前訂單到3個月前訂單增減量
        Openso4and5_diff = PART_NO_Openso_4.OPEN_QTY - PART_NO_Openso_5.OPEN_QTY
        data['Feature_53']= Openso4and5_diff.shift(-3).fillna(0)
        data['Feature_54']= Openso4and5_diff.shift(-2).fillna(0)
        data['Feature_55']= Openso4and5_diff.shift(-1).fillna(0)
        # output 
        data['Output'] = data['ORDER_QTY']
        data = data.merge(Trend_Indicator, on='Date', how='left')
        data = data.fillna(0)
        return data

    def input_output_data(self, data, input_columns, output_columns, output_idx=2, use_all=True):
        inputs = []
        outputs = []
        
        if use_all:
            index = data.index
        else:
            # 找出 labels 為 1 -1 的列的索引位置
            index = data[(data['labels'] == 1) | (data['labels'] == -1)].index
            
        for idx in index:
            if idx + output_idx < len(data):
                inputs.append(data.loc[idx, input_columns])
                outputs.append(data.loc[idx + output_idx, output_columns])

        input_data = pd.DataFrame(inputs, columns=input_columns)
        output_data = pd.DataFrame(outputs, columns=output_columns)
        return input_data, output_data

    def feature_process(self, PART_NO_Openso_1, PART_NO_Openso_2, PART_NO_Openso_3,PART_NO_Openso_4 ,PART_NO_Openso_5, PART_NO_SO, Trend_Indicator, input_columns, output_columns, y_idx=2):
        caculate_data = self.feature_data(PART_NO_Openso_1, PART_NO_Openso_2, PART_NO_Openso_3, PART_NO_Openso_4 ,PART_NO_Openso_5, PART_NO_SO, Trend_Indicator)
        input_data, output_data = self.input_output_data(caculate_data, input_columns, output_columns, output_idx=y_idx, use_all=True)
        return input_data, output_data