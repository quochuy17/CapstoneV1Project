# Import the needed libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb
import datetime as dt
import numpy as np

# Import the Pyspark resources
from pyspark.sql.functions import date_add as d_add
from pyspark.sql.types import DoubleType, StringType, IntegerType, FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql import Row

from pyspark.sql.functions import avg
from pyspark.sql.functions import weekofyear,date_format, col, udf, dayofmonth, dayofweek, month, year
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

#Import the needed main file
import main as main

#Define a function "display_parquet_status" for list out the status of the parquet
def display_parquet_status(df_intp, output_directory):
    """
        Main context : Starting write down the files on the parquet after obtaining status information from fact tables.
                The used parameters including:
                    + "df_intp": display the inputed data with the format on the dataframe.
                    + "return": get the status of the dimension dataframe.
                    + "output_directory": list out the direct path for write down the rows of data after finishing the process.     
    """    
    output_df = df_intp.withColumn("status_flag_id", monotonically_increasing_id()) \
                .select(["status_flag_id", "entdepa", "entdepd", "matflag"]) \
                .withColumnRenamed("entdepa", "arrival_flag")\
                .withColumnRenamed("entdepd", "departure_flag")\
                .withColumnRenamed("matflag", "match_flag")\
                .dropDuplicates(["arrival_flag", "departure_flag", "match_flag"])
    main.output_to_parquet_file(output_df, output_directory, "status")
    return output_df

#Define a function "collect_fact_time" to gather the time from fact table after extracting some of fields like year, month, day.
def collect_fact_time(df_intp, output_directory):   
    """
        Main story : Just recognize the information of time from the fact, then write the year, month, day, week, and weekday on the parquet.
                    The used parameters including:
                        + "df_intp": display the inputed data with the format on the dataframe.
                        + "output_directory": list out the direct path for write down the rows of data after finishing the process. 
                        + "return": get the status of the dimension dataframe.
    """
    #import the extra libraries
    from datetime import datetime, timedelta
    from pyspark.sql import types as T
    
    #Define function "convert_datetime" for converting the current datetime to accurate the datetime.
    def convert_datetime(x):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except:
            return None
    
    convert_udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    output_df = df_intp.select(["arrdate"])\
                .withColumn("arrival_date", convert_udf_datetime_from_sas("arrdate")) \
                .withColumn('day', F.dayofmonth('arrival_date')) \
                .withColumn('month', F.month('arrival_date')) \
                .withColumn('year', F.year('arrival_date')) \
                .withColumn('week', F.weekofyear('arrival_date')) \
                .withColumn('weekday', F.dayofweek('arrival_date'))\
                .select(["arrdate", "arrival_date", "day", "month", "year", "week", "weekday"])
    main.output_to_parquet_file(output_df, output_directory, "time")
    return output_df

#Define a function "collect_visa_info" to gather the information of the visa and then display on parquet files.
def collect_visa_info(df_intp, output_directory):
    """
        Main context : Just gather the information of visa in the detailed level, then create dataframe and write data into parquet files.
        The used parameters including:
            + "df_intp": display the inputed data with the format on the dataframe.
            + "output_data": showing the directory path to write data.
            + "return": get the status of the dimension dataframe and then presenting dataframe though dimension.
    """
    output_df = df_intp.withColumn("visa_id", monotonically_increasing_id()) \
                .select(["visa_id","i94visa", "visatype", "visapost"]) \
                .dropDuplicates(["i94visa", "visatype", "visapost"])
    main.output_to_parquet_file(output_df, output_directory, "visa")
    return output_df

#Define a function "collect_state_info" to gather the information of the state, state code and then show on parquet files.
def collect_state_info(df_intp, output_directory):
    """
        Main context : Collect the state specific data and then create dataframe and write data into parquet files.
        The used parameters including:
            + "df_intp": display the inputed data with the format on the dataframe.
            + "output_data": showing the directory path to write data.
            + "return": get the status of the dimension dataframe and then presenting dataframe though dimension.
    """
    
    output_df = df_intp.select(["State Code", "State", "Median Age", "Male Population", "Female Population", "Total Population", "Average Household Size",\
                          "Foreign-born", "Race", "Count"])\
                .withColumnRenamed("State","state")\
                .withColumnRenamed("State Code", "state_code")\
                .withColumnRenamed("Median Age", "median_age")\
                .withColumnRenamed("Male Population", "male_population")\
                .withColumnRenamed("Female Population", "female_population")\
                .withColumnRenamed("Total Population", "total_population")\
                .withColumnRenamed("Average Household Size", "avg_household_size")\
                .withColumnRenamed("Foreign-born", "foreign_born")\
                .withColumnRenamed("Race", "race")\
                .withColumnRenamed("Count", "count")
    
    output_df = output_df.groupBy("state_code","state").agg(
                mean('median_age').alias("median_age"),\
                sum("total_population").alias("total_population"),\
                sum("male_population").alias("male_population"), \
                sum("female_population").alias("female_population"),\
                sum("foreign_born").alias("foreign_born"), \
                sum("avg_household_size").alias("average_household_size")
                ).dropna()
    
    main.output_to_parquet_file(output_df, output_directory, "state")
    return output_df

#Define a function "create_airport_info" to gather the information of the airport data, and then show on parquet files.
def create_airport_info(df_intp, output_directory):
    """
        Main context : Collect the airport data, and then create the dataframe for displaying and write data into parquet files.
        The used parameters including:
            + "df_intp": display the inputed data with the format on the dataframe.
            + "output_data": showing the directory path to write data.
            + "return": dataframe representing airport dimension.
    """
    output_df = df_intp.select(["ident", "type", "iata_code", "name", "iso_country", "iso_region", "municipality", "gps_code", "coordinates", "elevation_ft"])\
                .dropDuplicates(["ident"])
    
    main.output_to_parquet_file(output_df, output_directory, "airport")
    return output_df

#Define a function "create_temperature_info" to gather the information of the temperature data.
def create_temperature_info(df_intp, output_directory):
    """
        Main context : Gathering the data of temperature, and then collect the average temperature via grouping by country.
        The used parameters including:
            + "df_intp": display the inputed data with the format on the dataframe.
            + "output_data": showing the directory path to write data.
            + "return": dataframe representing temperature dimension.
    """
    print("creating temperature table data")
    output_df = df_intp.groupBy("Country","country").agg(
                mean('AverageTemperature').alias("average_temperature"),\
                mean("AverageTemperatureUncertainty").alias("average_temperature_uncertainty")
            ).dropna()\
            .withColumn("temperature_id", monotonically_increasing_id()) \
            .select(["temperature_id", "country", "average_temperature", "average_temperature_uncertainty"])
    
    main.output_to_parquet_file(output_df, output_directory, "temperature")
    return output_df

#Define a function "create_country_info" to gather the information of the country data, and create the dataframe and then show on parquet files.
def create_country_info(df_intp, output_directory):
    """
        Main story : Collect country data, create dataframe and write data into parquet files.
        The used parameters including:
            + "df_intp": dataframe of input data.
            + "output_directory": path of output parquet file.
            + "return": returning the country dimension data.
    """
    main.output_to_parquet_file(df_intp, output_directory, "country")
    return df_intp

#Define a function "create_immigration_info" to gather the information of the country data, and create the dataframe and then show on parquet files.
def create_immigration_info(immigration_spark, output_directory, spark):
    """
        Main context : Collect all immigration data, join dimension tables, create data frame and write to parquet file.  
        The used parameters including:
            + "immigration_spark": display the inputed data about the immigration and using the spark for reading the data.
            + "output_directory": showing the directory path to write data and the directory to the parquet files.
            + "return": returing the final immigration fact data frame.
    """
    airport = spark.read.parquet(output_path+"airport")
    country = spark.read.parquet(output_path+"country")
    temperature = spark.read.parquet(output_path+"temperature")
    country_temperature = spark.read.parquet(output_path+"country_temperature_mapping")
    state = spark.read.parquet(output_path+"state")
    status = spark.read.parquet(output_path+"status")
    time = spark.read.parquet(output_path+"time")
    visa = spark.read.parquet(output_path+"visa")

    # join all tables to immigration for returning the correct results
    output_df = immigration_spark.select(["*"])\
                .join(airport, (immigration_spark.i94port == airport.ident), how='full')\
                .join(country_temperature, (immigration_spark.i94res == country_temperature.code), how='full')\
                .join(status, (immigration_spark.entdepa == status.arrival_flag) & (immigration_spark.entdepd == status.departure_flag)\
                    & (immigration_spark.matflag == status.match_flag), how='full')\
                .join(visa, (immigration_spark.i94visa == visa.i94visa) & (immigration_spark.visatype == visa.visatype)\
                    & (immigration_spark.visapost == visa.visapost), how='full')\
                .join(state, (immigration_spark.i94addr == state.state_code), how='full')\
                .join(time, (immigration_spark.arrdate == time.arrdate), how='full')\
                .where(col('cicid').isNotNull())\
                .select(["cicid", "i94res", "depdate", "i94mode", "i94port", "i94cit", "i94addr", "airline", "fltno", "ident", "code",\
                         "temperature_id", "status_flag_id", "visa_id", "state_code", country_temperature.country,time.arrdate.alias("arrdate")])
    return output_df