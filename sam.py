#from hdfs import InsecureClient



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType 
from pyspark.sql.functions import col
if __name__ == '__main__':

    #Creating spark session
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
