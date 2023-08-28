#from hdfs import InsecureClient



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType 
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.appName('demo').getOrCreate()

    #Creating list of data to create dataframe
    person_list = [('Berry','','Allen',1,'M'),
                    ('Oliver','Queen','',2,'M'),
                    ('Robert','','Williams',3,'M'),
                    ('Tony','','Stark',4,'F'),
                    ('Rajiv','Mary','kumar',5,'F')
                    ]

    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", IntegerType(), True), \
        StructField("gender", StringType(), True)   
    ])

    df=spark.createDataFrame(data=person_list,schema=schema)
    df.show(truncate=False)
    df.printSchema()

    '''df1=spark.read.csv('/config/workspace/record.csv')
    df1.show(truncate=False)
    df1.printSchema()'''

    df2=spark.read.option('header',True).csv('/departments.csv')
    df2.show(truncate=False)
    df2.printSchema()

    df3=spark.read.option('header',True).option('inferSchema',True).csv('/employees.csv')
    df3.show(truncate=False)
    df3.printSchema()

    df3.select('*').show()
    df3.select('EMPLOYEE_ID','FIRST_NAME').show()
    df3.select(df3.EMPLOYEE_ID,df3.FIRST_NAME).show()
    df3.select(df3['EMPLOYEE_ID'],df3['FIRST_NAME']).show()

    df3.select(col('EMPLOYEE_ID'),col('FIRST_NAME')).show()


    
    df3.select(col('EMPLOYEE_ID').alias('emp_id'),col('FIRST_NAME').alias('f_name')).show()
    df3.select('EMPLOYEE_ID','FIRST_NAME','SALARY').withColumn('NEW_SALARY',col('SALARY')+1000).show()

    df3.write.format("parquet").mode("overwrite").option("compression", "gzip").save("/copy_from_pyspark")

    #hdfs dfs -cat /copy_from_pyspark/*
    print('-------------------------------')
    
    df4=spark.read.option('header',True).option('inferSchema',True).parquet('/copy_from_pyspark')
    print('-------------------------------')
    df4.show(truncate=False)
    print('-------------------------------')
    df4.printSchema()

    df3.withColumn('SALARY', col('SALARY')-1000).select('EMPLOYEE_ID','FIRST_NAME','SALARY').show()
    print('-------------------------------')
    print('-------------------------------')
    df3.withColumn('new_SALARY', col('SALARY')-1000).select('EMPLOYEE_ID','FIRST_NAME','new_SALARY').show()

    df3.withColumnRenamed('SALARY','EMP_SALARY').show()

    df3.drop('COMMISSION_PCT').show()

    df3.printSchema()

    df3.filter(col('SALARY')<5000).show()

    df3.filter(col('SALARY')<5000).show(10)

    df3.filter(col('SALARY')<5000).select("EMPLOYEE_ID","FIRST_NAME","SALARY").show(10)

    df3.filter((col('DEPARTMENT_ID')==50) & (col('SALARY')<5000)).select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(10)

    df3.filter('DEPARTMENT_ID<>50').select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(10)

    df3.filter('DEPARTMENT_ID != 50').select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(12)

    df3.filter("DEPARTMENT_ID == 50 and SALARY < 5000").select("EMPLOYEE_ID","FIRST_NAME","SALARY","DEPARTMENT_ID").show(10)

    df3.distinct().show()

    df3.dropDuplicates().show(12)

    df3.dropDuplicates(["DEPARTMENT_ID", "HIRE_DATE"]).show(10)

    df3.dropDuplicates(["DEPARTMENT_ID", "HIRE_DATE"]).select("EMPLOYEE_ID","HIRE_DATE","DEPARTMENT_ID").show(11)

    df3.dropDuplicates(["DEPARTMENT_ID", "HIRE_DATE"]).select("EMPLOYEE_ID").show(11)

    
    rows = df3.count()
    print(f"DataFrame Rows count : {rows}")

    df3.select(count('SALARY')).show()

    df3.select(count('SALARY').alias('total_count')).show()

    df3.select(max('SALARY').alias('max_salary')).show()
    df3.select(min('SALARY').alias('min_salary')).show()
    df3.select(avg('SALARY').alias('avg_salary')).show()
    df3.select(sum('SALARY').alias('sum_salary')).show()
    

    df3.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy('SALARY').show(5)
    #mpDf.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy("salary").show()

    df3.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy(col('DEPARTMENT_ID').asc()).show(6)
    
    df3.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy(col('DEPARTMENT_ID').asc(),col('SALARY').desc()).show(7)
    
    df3.select("EMPLOYEE_ID","FIRST_NAME","DEPARTMENT_ID","SALARY").orderBy(col('DEPARTMENT_ID').asc(),col('SALARY').asc()).show(7)

    df3.groupBy('DEPARTMENT_ID').sum('SALARY').show(8)

    df3.groupBy('DEPARTMENT_ID').max('SALARY').show(5)
    df3.groupBy('DEPARTMENT_ID').min('SALARY').show(6)

    df3.groupBy("DEPARTMENT_ID","JOB_ID").sum('SALARY').show(8)

    df3.printSchema()

    df3.show()
    df3.groupBy("DEPARTMENT_ID","JOB_ID").sum('SALARY').show()

    df3.groupBy("DEPARTMENT_ID","JOB_ID").sum('SALARY', "EMPLOYEE_ID").show(7)
    df3.groupBy("DEPARTMENT_ID","JOB_ID").sum('DEPARTMENT_ID','SALARY', "EMPLOYEE_ID").show(8)

    df3.groupBy('DEPARTMENT_ID').agg(sum("SALARY").alias("SUM_SALARY") ,\
    max("SALARY").alias("MAX_SALARY"), min("SALARY").alias("MIN_SALARY") , avg("SALARY").alias("AVG_SALARY")).show()

    df = df3.withColumn("EMP_GRADE", when( col("SALARY") > 15000 , "A").when( (col("SALARY") >= 10000) & ( col("SALARY") < 15000), "B").otherwise("C"))
    df.select("EMPLOYEE_ID", "SALARY", "EMP_GRADE").show(10)

