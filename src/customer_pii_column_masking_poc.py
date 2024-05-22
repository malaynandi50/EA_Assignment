# Databricks notebook source
################################################################################################################################
#### This program will read the csv file present in input DBFS location which needs to be provided in file path and it does ####
#### data masking for PII column like doingbusinessasname, email and mobileno                                               ####
####                                                                                                                        ####
#### Input file expected in CSV format                                                                                      ####
#### Original input file written to DBFS location and stored as CSV format                                                  ####
#### Final output file written to DBFS location and stored as parquet format                                                ####
################################################################################################################################
from pyspark.sql.functions import udf, col, lit, coalesce

def mask_email(email):
    if '@' in email:
        at_index_pos = email.index('@')
        return email[0] + "*" * (at_index_pos - 2) + email[at_index_pos - 1:]
    else:
        return email

def mask_mobile(mobile):
    return mobile[0] + "*" * (len(mobile) - 2) + mobile[-1]

# Register the UDF 
mask_email_udf = udf(mask_email)
mask_mobile_udf = udf(mask_mobile)

if __name__ == "__main__":
    file_path = 'dbfs:/FileStore/tables/EA/sample_pcc_pcc_customers_17_05_202405201521.csv'
    df = spark.read.format('csv').option("delimiter",',').option('header','True').load(file_path)

    # Write the input file to DBFS in original csv format
    df.write.mode("overwrite").csv('dbfs:/FileStore/tables/EA/Input/customer_raw_file.csv')
   
    # call udf to mask email and mobileno column and  replace original value by '****' in doingbusinessasname column
    df1 = df.withColumn("email", coalesce(col("email"), lit("*"))).\
        withColumn("mobileno", coalesce(col("mobileno"), lit("****"))).\
        withColumn("doingbusinessasname", lit("*"))

    df1 = df1.withColumn("email", mask_email_udf(col('email'))) \
        .withColumn("mobileno", mask_mobile_udf("mobileno")) 
    
    # Write the output dataframe and store it in parquet file format        
    df1.write.mode("overwrite").parquet('dbfs:/FileStore/tables/EA/output/customer_masked_pii.parquet')
   
