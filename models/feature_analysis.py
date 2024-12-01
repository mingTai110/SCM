import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy.stats import skew, kurtosis


def feature_distribution(Trend_Label_data):
    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(111, projection='3d')

    # 定義 Z 軸的 threshold
    lower_threshold = -0.5
    upper_threshold = 0.5

    # 根據 Z 值的範圍指定顏色
    z_values = Trend_Label_data['Output_category']
    colors = np.where(
        z_values < lower_threshold, 'blue',      # 小於下限為藍色
        np.where(z_values > upper_threshold, 'red', 'green')  # 大於上限為紅色，介於範圍內為綠色
    )

    # 繪製三維散點圖，使用指定顏色
    scatter = ax.scatter(
        Trend_Label_data['Indicator_1(normalrize)'], 
        Trend_Label_data['Indicator_2(normalrize)'], 
        z_values, 
        c=colors, 
        marker='o'
    )

    # 設定軸標籤
    ax.set_xlabel('Indicator_1(normalrize)')
    ax.set_ylabel('Indicator_2(normalrize)')
    ax.set_zlabel('Normalrized_Diff')
    ax.set_xlim(-1, 10)
    ax.set_ylim(-1, 10)
    plt.show()

def Error_Distribution(Trend_Label,combine_list):
    error_data=pd.DataFrame()
    error_data['Labels']=Trend_Label
    error_data['Error']=np.array(combine_list)
    upward_data=error_data[error_data['Labels']==1].reset_index(drop=True)
    downward_data=error_data[error_data['Labels']==-1].reset_index(drop=True)
    print("All_data _mean",error_data['Error'].mean())
    print("All_data _Error_std",error_data['Error'].std())
    print("Upward_data _Error_mean: ",upward_data['Error'].mean())
    print("Upward_data _Error_std: ",upward_data['Error'].std())
    print("Downward_data _Error_mean: ",downward_data['Error'].mean())
    print("Downward_data _Error_std: ",downward_data['Error'].std())
    upward_skewness = skew(upward_data['Error'])
    upward_kurtosis = kurtosis(upward_data['Error'])
    downward_skewness = skew(downward_data['Error'])
    downward_kurtosis = kurtosis(downward_data['Error'])

    plt.figure(figsize=(10, 6))
    # 绘制上升误差的直方图和 KDE 曲线
    plt.hist(upward_data['Error'], bins=15, color='skyblue', edgecolor='black', alpha=0.5, density=True, label='Upward Error')
    sns.kdeplot(upward_data['Error'], color='blue')
    # 绘制下降误差的直方图和 KDE 曲线
    plt.hist(downward_data['Error'], bins=15, color='salmon', edgecolor='black', alpha=0.5, density=True, label='Downward Error')
    sns.kdeplot(downward_data['Error'], color='red')
    plt.xlabel('Error Value')
    plt.ylabel('Density')
    plt.title(f'Error Distribution\n'
            f'Upward Skewness: {upward_skewness:.2f}, Kurtosis: {upward_kurtosis:.2f} | '
            f'Downward Skewness: {downward_skewness:.2f}, Kurtosis: {downward_kurtosis:.2f}')
    plt.legend()
    plt.show()