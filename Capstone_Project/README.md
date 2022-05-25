# Udacity Provided Capstone Project  

In the Udacity provided project, the main data includes data on immigration to the United States, and supplementary datasets include data on airport codes, U.S. city demographics, and temperature data. Using this data, we create data lake tables using Pyspark. Created data lake tables can be used to analyze different queries such as immigration trends at destination cities, gender or age distribution of the immigrants, or visa distribution of the immigrants. We use 3 different sources, the immigration dataset, city temperature and demographics data. 

The project follows 5 steps:

- Step 1: Scope the Project and Gather Data
- Step 2: Explore and Assess the Data
- Step 3: Define the Data Model
- Step 4: Run ETL to Model the Data
- Step 5: Complete Project Write Up

### Datasets
We use the following datasets for this project:

1. **I94 Immigration Data:** This data comes from the US National Tourism and Trade Office. the original data source is: https://travel.trade.gov/research/reports/i94/historical/2016.html. The dataset within sas_data contains data from 2016. A data dictionary and also a sample csv data exist.

2. **World Temperature Data:** This data comes from Kaggle and contains average weather temperatures by city. The source link is: https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data 

3. **U.S. City Demographic Data:** This data comes from OpenSoft and contains information about the demographics of US cities such as average age, male and female population. The source link: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

### Data Exploration and Data Model

Before creating our tables, first we explore the data and do some cleansing as necessary. For example, we drop invalid records, remove any missing values do some data conversion (e.g. time format) and drop dublicates. We explore and clean the data for immigration, temperature and demographics data. 

After cleaning the data, we have created the following fact and dimension tables. For our data model we used Star Schema model. The reason for that is start schema databases are best used for historical and large datasets. This makes them work most optimally for making queries. 

#### Dimension tables

`immigrant table`
- id
- gender
- age
- visa_type

`city table`
- city_code
- state_code
- city_name
- median_age
- pct_male_pop
- pct_female_pop
- pct_veterans
- pct_foreign_born
- pct_native_american
- pct_asian
- pct_black
- pct_hispanic_or_latino
- pct_white
- total_pop
- lat
- long


`monthly city temperature table`
- city_code
- year
- month
- avg_temperature

`time table`
- date
- dayofweek
- weekofyear
- month

#### Fact table

`immigration table`
- id
- state_code
- city_code
- date
- count

### ETL Steps

ETL performs following steps to ingest, clean and save the data:

1. Load immigration, demographics and temperature datasets.
2. Clean each dataset for invalid/incorrect records, missing values or duplicates etc.
3. Load staging tables for each dataset.
4. Create dimension tables immigrant, city, city_temp and time. 
5. Create fact table immigration.
6. Perform data quality checks by checking if each table has records in it.
7. Save dataframes in Parquet format.


### Data Dictionary
Here are the data dictionaries for main tables.

**Fact table `immigration`focuses on events/facts on immigration process and contains the following variables:** <br>
|-- id: id from sas file<br>
|-- state_code: of the arrival city<br>
|-- city_code: city port of arrival city<br>
|-- date: date of arrival in U.S.<br>
|-- count: count of immigrant's entries into the US<br>

**Dimension table `immigrant` focuses on people who immigrated and contains the following variables:**<br>
|-- id: immigrant's id<br>
|-- gender: immigrant's gender<br>
|-- age: immigrant'sage<br>
|-- visa_type: immigrant's visa type<br>


**Dimension table `city` focuses on demographic character of the cities and contains the following variables:**<br>
|-- city_code: code of the city<br>
|-- state_code: code of the state<br>
|-- city_name: city's name<br>
|-- median_age: median age of the population living on that city<br>
|-- pct_male_pop: percentage of male population<br>
|-- pct_female_pop: percentage of female population<br>
|-- pct_veterans: percentage of veterans<br>
|-- pct_foreign_born: percentage of foreigners<br>
|-- pct_native_american: percentage of native americans<br>
|-- pct_asian: percentage of asian population<br>
|-- pct_black: percentage of black people<br>
|-- pct_hispanic_or_latino: percentage of hispanic or latino people<br>
|-- pct_white: percentage of white people<br>
|-- total_pop: total population<br>
|-- lat: latitude of the city<br>
|-- long: longitude of the city<br>


**Dimension table `monthly city temperature` focuses on temperature values on a particular city and contains the following variables:**<br>
|-- city_code: city port code<br>
|-- year: year <br>
|-- month: month of the year <br>
|-- avg_temperature: average temperature of a city in a given month<br>

**Dimension table `time` contains the following time related variables:**<br>
|-- date: date<br>
|-- dayofweek: day of the week<br>
|-- weekofyear: week of year<br>
|-- month: month<br>

### Discussion
We have chosen Spark as our data processing engine because Spark executes much faster by caching data in memory across multiple parallel operations. Therefore it is a good choice to process big data sets by splitting the work up into chunks and assigning those chunks accross computational resources. In order to read the data into a data frame, we have used Pandas library. The final tables are written in columnar format using Parquet.
 
The data update cycle depends on the availability of new data. Frequent update for such a big dataset might be a bit cumbersome, since collecting those infos is not that easy. Since immigration and temperature raw datasets built up monthly, we would recommend monthly update.

**If the data was increased by 100x:** First of all, we need to increase the performance. There are several ways to increase the Spark performance such as workload type, partitioning scheme, or memory consumption. We can start to increase number of executers and as a result, more worker nodes in cluster can process the data. 

**If the data populates a dashboard that must be updated on a daily basis by 7am every day:** We can use Apache Airflow to schedule a job at 7 am daily which initiates a Spark job/task. 

**If the database needed to be accessed by 100+ people:** In this case, our system should scale because of increased amount of users. Scalability means an ability to handle more users, clients, data, transactions, or requests without affecting the user experience by adding more resources. For that, we should host our system in a data warehouse on cloud infrastructure (e.g. Amazon Redshift) which provides larger capacity to serve mass consumption. 