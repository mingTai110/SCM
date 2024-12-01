
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from models.model_performance import test_data_performance,plt_fcst,fcst_performance


def Model_Predict(Train_Input_n,Train_Output_n, Valid_Input_n, Valid_Output_n):
    X_train = Train_Input_n
    y_train =Train_Output_n['Normalrized_Diff_two_month'].fillna(0)
    X_test = Valid_Input_n
    y_test =Valid_Output_n['Normalrized_Diff_two_month'].fillna(0)

    # 建立並訓練隨機森林回歸模型
    rf_regressor = RandomForestRegressor(random_state=42)
    rf_regressor.fit(X_train, y_train)
    # 預測
    y_train_pred = rf_regressor.predict(X_train)
    y_test_pred = rf_regressor.predict(X_test)
    # 評估模型
    train_mse = mean_squared_error(y_train, y_train_pred)
    test_mse = mean_squared_error(y_test, y_test_pred)
    print(f'Training MSE: {train_mse:.2f}')
    print(f'Testing MSE: {test_mse:.2f}')
    plt_fcst(y_test,y_test_pred)

    test_result=test_data_performance(Valid_Input_n, Valid_Output_n,y_test_pred)
    test_acc=fcst_performance(test_result)
    return test_acc