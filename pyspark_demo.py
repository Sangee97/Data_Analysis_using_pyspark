#Task 1: Establish PySpark Connection
from pyspark.sql import SparkSession
from pyspark.sql.functions import max,col,count

spark = SparkSession.builder.appName("BatchProcessing").getOrCreate()

#Task 2: Load Data into PySpark DataFrames
sales_data = spark.read.csv("Sales.csv", header=True,inferSchema=True)
cus_data= spark.read.csv("Customer.csv", header=True,inferSchema=True)
prod_data= spark.read.csv("Product.csv", header=True,inferSchema=True)
#task 3:Rename columns for consistency if needed
cus_data= cus_data.withColumnRenamed("Customer ID", "customer_id")
sales_data=sales_data.withColumnRenamed("Customer ID", "customer_id")
# cus_data.show()

#to know the datatypes of column:
#cus_data.printSchema()

#join the df

sale_prod=sales_data.join(prod_data, "Product ID")
cus_sale=sales_data.join(cus_data,'customer_id')
prod_cus=cus_sale.join(prod_data,'Product ID')
# prod_cus.show()
# cus_sale.show()

#qn 1:What is the total sales for each product category?
# sale_prod.groupby('Category','Sub-Category').sum('Sales').show()

#qn:2
#cus_sale.groupby('Customer Name').sum('Sales')
# max_value = cus_sale.select(max("Sales").alias("max_sales")).collect()[0]["max_sales"]
# cus_name= cus_sale.filter(col("Sales") == max_value).select("Customer Name").collect()[0]["Customer Name"]
# print(cus_name)
# print(max_value)

#qn 3:What is the average discount given on sales across all products?
#sales_data.groupby('Product ID').avg('Discount').show()

#4:How many unique products were sold in each region?
#prod_cus.groupby('Region').agg(count('Product Name')).show()

#5: What is the total profit generated in each state?
#prod_cus.groupby('State').sum('Sales').show()

#6:Which product sub-category has the highest sales?
# prod_cus.groupby('Sub-Category').sum('Sales')
# max_v=prod_cus.select(max('Sales')).collect()[0][0]
# max_row=prod_cus.filter(prod_cus['Sales']==max_v)
# max_row.show()

#7:What is the average age of customers in each segment?
#cus_data.groupby('Segment').avg('Age').show()

#8:How many orders were shipped in each shipping mode?
#sales_data.groupby('Ship Mode').count().show()

#9:What is the total quantity of products sold in each city?
#prod_cus.groupby('City').sum("Quantity").show()

#10:Which customer segment has the highest profit margin?
# cus_sale.groupby('Segment').sum('Profit').show()
# max_vl=cus_sale.agg(max('Profit')).collect()[0][0]
# mx_r=cus_sale.filter(cus_sale['Profit']==max_vl)
# mx_r.show()


