create database Automobile;

use Automobile;

Creating Tables-

create table Vehicles( vehicle_ID int not null,
 primary key (vehicle_ID),
 Model varchar(100),
 Make varchar(100),
 Year int,
 Engine_type varchar(100),
 Price Double);
 
 create table Sales( Sale_ID int primary key,
 Vehicle_ID int,
 Sale_Date date,
 Dealer varchar(100),
 Sale_Price float,
 Foreign key(vehicle_ID) references Vehicles(vehicle_ID));
 
 create table Customers( Customer_ID int primary key,
 Name Varchar(100),
 Age int, 
 Gender Varchar(100),
 Location Varchar(100), 
 vehicle_ID int,
 foreign key(Vehicle_ID) references Vehicles(Vehicle_ID));
 
  
LOAD DATA INFILE 'vehicles.csv' into table Vehicles;
LOAD DATA INFILE 'sales.csv' into table Sales;
LOAD DATA INFILE 'Customers.csv' into table Customers;

select * from vehicles_sales_summary;

drop table vehicles_sales_summary;

--providing statistical insights 
1) Total_number_of_Vehicles
select Year, count(vehicle_ID) as Total_number_of_vehicles from vehicles_sales_summary group by year;

2) Average sale per make
select Make,avg(sale_price) as Avg_sale_per_make from vehicles_sales_summary group by Make;

3) Most popular vehicle model
select Model from vehicles_sales_summary order by vehicle_ID;

