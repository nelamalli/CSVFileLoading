'''
from pyspark.sql import SparkSession, DataFrame
spark1 = SparkSession.builder.appName('CSVFileLoading').getOrCreate()
df = spark1.read.csv("C:/Users/prave/onedrive/Desktop/Firedepartments.csv", inferSchema=True,header=True)  # type: DataFrame
print(df.printSchema())
print(df.show())
print(df.describe().show())
'''
from pandas import DataFrame

'''
from pyspark.sql.functions import current_date
df1 = df.withColumn("calldate", current_date())
df1.show()
'''

#output_df = df.withColumn("calldate",df["calldate"].cast('int'))
'''
print(output_df.printSchema())
print(output_df.show())
print(output_df.describe().show())
'''


import pandas
#pandas.read_csv("C:/Users/prave/onedrive/Desktop/Firedepartments.csv")
df = pandas.read_csv("C:/Users/prave/onedrive/Desktop/Firedepartments.csv", index_col=0,delimiter=',',)
print('before resetting the indexes and when index is set as column in Zero position')

print(df)

print('After resetting the indexes ')

df.reset_index(inplace=True)

print(df.dtypes)
#df_new = pandas.read_csv("C:/Users/prave/onedrive/Desktop/Firedepartments.csv")  #type:DataFrame

df_new = df[1:5]
print('Original DataFrame\n------------------')
print(df_new)
#print(df_new.info)
curr_date = pandas.to_datetime('today').date()
new_row =  {'CallNumber':2222222, 'IncidentNumber':111111, 'CallType': 'Pravee', 'CallDate': curr_date, 'WatchDate': curr_date, 'ReceivedDtTm': curr_date, 'EntryDtTm':curr_date,'CallFinal ': 'Other' ,'Unit':False}
#append row to the dataframe
df_new = df_new.append(new_row, ignore_index=True)

df_new.to_csv('.\praveen.csv')

print('\nNew row added to DataFrame\n--------------------------')
#print(df_new)
