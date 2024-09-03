import mysql.connector
import pandas as pd
import numpy as np

#connection of MYSQL with Python connector

mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  password="Narasimhan@98.",
  database="Automobile"
)

print(mydb)

#Execution of Queries with Pandas
Threshold = mydb.cursor()
Threshold.execute('use Automobile;')
Threshold.execute('Select * from Vehicles')
rows=Threshold.fetchall()
columns=[column[0] for column in Threshold.description]

#Creation of DataFrame
df=pd.DataFrame(rows,columns=columns)
#print(df.info())

#Performing Filtering of data
vehicles_sold_above_threshold=df[df['Price']>15000]

#print(vehicles_sold_above_threshold)
Threshold.execute('Select * from Sales')

#Performing Joining of data
rows1=Threshold.fetchall()
columns=[column[0] for column in Threshold.description]
df1=pd.DataFrame(rows1,columns=columns)
df2=pd.merge(vehicles_sold_above_threshold,df1,left_on=['vehicle_ID'],right_on=['Vehicle_ID'],how="inner")
#print(df2)

sum=df2.groupby(['vehicle_ID','Model','Make','Year','Engine_type']).agg({'Price':'sum'})
#print(sum)

Threshold.execute('Select * from Customers')
rows3=Threshold.fetchall()
columns=[column[0] for column in Threshold.description]
df3=pd.DataFrame(rows3,columns=columns)
df4=pd.merge(df2,df3,left_on=['vehicle_ID'],right_on=['vehicle_ID'],how="inner")
#print(df4)

#Performing Aggregation of data
avg=df4.groupby(['vehicle_ID','Model','Make','Year','Engine_type',],as_index=False).agg({'Sale_Price':'mean'})
#print(avg.info())

val=np.int32().item()
avg['vehicle_ID']=avg['vehicle_ID'].astype(object)
avg['Year']=avg['Year'].astype(object)

print(type(val))
data_records=avg.to_records()
#print(data_records)

#Transformation of Data into a Table
Threshold.execute("""
                     CREATE TABLE IF NOT EXISTS Vehicles_Sales_Summary 
                        (
                        vehicle_ID INT AUTO_INCREMENT PRIMARY KEY,
                        Model VARCHAR(100),
                        Make VARCHAR(100), 
                        Year int,
                        Engine_type VARCHAR(100),
                        Sale_Price float
                        )
                    """)
#Inserting the values
DML = """INSERT INTO Vehicles_Sales_Summary 
        (vehicle_ID, Model, Make, Year, Engine_type,Sale_Price)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
#performing for loop to load the data
for record in data_records:
  VAL = (record.vehicle_ID, record.Model,
         record.Make, record.Year, record.Engine_type, record.Sale_Price)
  Threshold.execute(DML, VAL)
mydb.commit()
mydb.close()



