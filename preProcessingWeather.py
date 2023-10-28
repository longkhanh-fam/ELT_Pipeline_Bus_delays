import pandas as pd
path = 'D:/Khanh_FDE04/Project/trash/Weathre/weather_description.csv'
data = pd.read_csv(path,on_bad_lines ='skip')
data.columns = data.columns.str.replace(' ', '_').str.lower()
data = data.fillna(method='bfill', axis=0).dropna()
data.to_csv('D:/Khanh_FDE04/Project/trash/mta_1706/weather_description.csv', index=False)