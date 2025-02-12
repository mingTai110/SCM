import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
from utils.tools import WACC

class ModelProcessor:
    def __init__(self):
        pass

    def normalrize_data(self, train_input, test_input, train_output, test_output):
        # Normalize features
        train_data = (train_input - train_input.mean()) / train_input.std()
        test_data = (test_input - train_input.mean()) / train_input.std()
        
        # Normalize output difference
        diff_mean = train_output['Output'].mean()
        diff_std = train_output['Output'].std()
        train_output['mean'] = diff_mean
        train_output['std'] = diff_std
        train_output['Normalrized_Output'] = (train_output['Output'] - diff_mean) / diff_std
        test_output['mean'] = diff_mean
        test_output['std'] = diff_std
        test_output['Normalrized_Output'] = (test_output['Output'] - diff_mean) / diff_std
        
        return train_data, test_data, train_output, test_output

    def plot_fcst(self, y_test, y_test_pred):
        # Plot the forecast results
        plt.figure(figsize=(10, 5))
        plt.plot(y_test.values, label='Actual')
        plt.plot(y_test_pred, label='Predicted')
        plt.legend()
        plt.show()


    def test_data_performance(self, Valid_Input_n, Valid_Output_n, y_test_pred):
        # Calculate the performance of the test data
        test_data = pd.concat([Valid_Input_n, Valid_Output_n], axis=1)
        test_data['y_pred'] = y_test_pred
        test_data['Pred_demand'] = round(test_data['y_pred'] * test_data['std'] + test_data['mean'])
        # test_data['Error_rate'] = np.abs((test_data['pred_demand'] - test_data['ORDER_QTY']) / test_data['ORDER_QTY']).clip(0, 1).fillna(0)
        # test_data['Pred_Acc'] = round((1 - test_data['Error_rate']) * 100, 2)
        # print('模型(含0)預測準確度', round(test_data['Pred_Acc'].mean(), 2))
        # print('此PD(含0)模型WACC準確度', round(WACC(test_data['Pred_Acc'], test_data['ORDER_QTY']), 2))
        return test_data

    def Model_Predict(self, Train_Input_n, Train_Output_n, Valid_Input_n, Valid_Output_n, model_name='random_forest', plot=True):
        X_train = Train_Input_n
        y_train = Train_Output_n['Normalrized_Output'].fillna(0)
        X_test = Valid_Input_n
        y_test = Valid_Output_n['Normalrized_Output'].fillna(0)

        # Build and train the model
        if model_name == "random_forest":
            print("use random forest model")
            model = RandomForestRegressor(random_state=42)
        else:
            print("use XGBoost model")
            model = XGBRegressor(random_state=42)
        
        model.fit(X_train, y_train)
        y_train_pred = model.predict(X_train)
        y_test_pred = model.predict(X_test)

        # Evaluate the model
        train_mse = mean_squared_error(y_train, y_train_pred)
        test_mse = mean_squared_error(y_test, y_test_pred)
        print(f'Training MSE: {train_mse:.2f}')
        print(f'Testing MSE: {test_mse:.2f}')

        if plot:
            self.plot_fcst(y_test, y_test_pred)

        test_result = self.test_data_performance(Valid_Input_n, Valid_Output_n, y_test_pred)
        return test_result[['labels', 'Pred_demand']]#, 'ORDER_QTY', 'Pred_Acc'

    def process_model(self, train_input_list, train_output_list, valid_input_list, valid_output_list, model_name='random_forest', plot=False):
        # Concatenate and fill missing values
        Train_Input = pd.concat(train_input_list, ignore_index=True).fillna(0)
        Valid_Input = pd.concat(valid_input_list, ignore_index=True).fillna(0)
        Train_Output = pd.concat(train_output_list, ignore_index=True)
        Valid_Output = pd.concat(valid_output_list, ignore_index=True)

        # Normalize data
        Train_Input_n, Valid_Input_n, Train_Output_n, Valid_Output_n = self.normalrize_data(Train_Input, Valid_Input, Train_Output, Valid_Output)
        Train_Input_n = Train_Input_n.fillna(0)
        Valid_Input_n = Valid_Input_n.fillna(0)

        # Predict using the model
        fcst_data = self.Model_Predict(Train_Input_n, Train_Output_n, Valid_Input_n, Valid_Output_n, model_name=model_name, plot=plot)
        return fcst_data