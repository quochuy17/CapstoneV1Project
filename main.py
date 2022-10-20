# Import the needed libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Import the Pyspark resources
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import dayofmonth, dayofweek, month, year, weekofyear, isnan, when, count, col, udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import avg
from pyspark.sql.types import *

# Import the datetime library
from datetime import datetime

def listout_unique_data(df,df_name,column_list):
    """
        Main context : The core goal is set a general utility API like this that may be used to determine how many distinct values are present in a column with the following parameters:
            + "df" : the dataframe which is collected and used in the project
            + "df_name" : showing the dataframe's name
            + "col_list": examining the columns whether it consists of the unique values and showing this results in the list.
    """
    
    print(f"The number of unique values for given columns are presented in the {df_name} dataframe.")
    for column in col_list:
        unique_values = df[column].unique()
        print(f"As the result, there are approximately {unique_values.size} unique values displayed in the column {column}.")
        
def remove_error_values(df,col_list):
    """
        Main context : As regard to the columns which are listed in the "col list" site, just check whether the rows contain any null values, and then eliminate any rows like that. The parameters used including:
            + "df" : is the dataframe's name, like above mention.
            + "col_list": checking the columns whether it contains the unique values and displaying this results in the list.
    """
    print(f"Starting the process of removing the rows containing the null values for the {col_list}")
    # use the function "Shape" to list out the removed rows before the process of cleaning.
    print(f"Before cleaning up, the total rows consists of {df.shape[0]} rows.")
    for column in col_list:
        bool_series = pd.notnull(df[column])
        df = df[bool_series]
    # use the function "Shape" to list out the removed rows after finishing the process of cleaning.
    print(f"After process of cleaning, the total rows include {df.shape[0]} rows.")
    return df

## Implement the process of parqueting data 
def parquet_outputfile(df, output_address, table_info):
    """
        Main context : creates a parquet file from the dataframe and then write all parqueting files.
            + "df": is the dataframe which is prepared to write down.
            + "table_info": showing the name and the information of the used table.
            + "output_address": pointing the directory of the output path after process of parqueting all related files and then confirm the place to store these processed files.
    """
    file_directory = output_address + table_info
    print("Start the process of writing the table {} to {}".format(table_info, file_directory))
    df.write.mode("overwrite").parquet(file_directory)
    print("The process of writing down the parqueting files is finished.")

# Implement the data sanitation process 
def run_record_count_check(df, table_info):
    """ Main story : For the process of data sanity, evaluate the counted rows. And then make the report with error information if no records were found. 
                     The used parameters uncluding : 
                                        + "input_df": spark dataframe to check counts on.
                                        + "table_info": showing the name and the information of the used table.
    """
    count = df.count()
    # If no records were founds, just make a notification with the detailed name of table
    if (count == 0):
        print("As the result, the number of failed records counted for {} table are zero records.".format(table_info))
    # By contrast, If it contains the records, then create the related notification displaying the detailed name of table and the number of corresponding records.
    else:
        print("As the result, the number of passed records counted for {} table contains about: {} records.".format(table_info, count))
    return 0    