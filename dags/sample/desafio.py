from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'Maycon',
    'email': ['mcn630@gmail.com'],
    'start_date': days_ago(1),
    'email_on_failure' : False
}

with DAG(
    dag_id = 'desafio-dag',
    default_args = default_args,
    catchup=False,
    max_active_runs = 1,
    schedule_interval = '0 5 * * *',
    tags=['covid19']
) as dag:

    def covid_raw_pipeline(file, type, repartition, previous_date, execution_date):

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, split, monotonically_increasing_id, row_number, regexp_replace, to_timestamp, sum, year, month, lit
        from pyspark.sql import Row
        from pyspark.sql.window import Window

        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("airflow_app") \
            .config('spark.executor.memory', '5g') \
            .config('spark.driver.memory', '5g') \
            .config("spark.driver.maxResultSize", "1048MB") \
            .config("spark.port.maxRetries", "100") \
            .getOrCreate()

        # JINJA TEMPLATES
        dt_previa = previous_date
        dt_execucao = execution_date
        
        covid1 = spark.read.options(inferSchema='true', header='true',sep=',').csv(f'/home/airflow/datalake/landing/covid19/{file}')

        covid1 = covid1.withColumnRenamed('Country/Region','pais') \
            .withColumnRenamed('Province/State','estado') \
            .withColumnRenamed('Lat','latitude') \
            .withColumnRenamed('Long','longitude') \

        covid2 = covid1.select('pais'
            , 'estado'
            , 'latitude'
            , 'longitude'
            , *(col(c).cast("string").alias(c) for c in covid1.columns if c not in {'pais', 'estado', 'latitude', 'longitude'}))

        covid2.registerTempTable('covid2')

        l = []
        separator = ', '
        A = list(covid2.select(*(col(c) for c in covid2.columns if c not in {'pais', 'estado', 'latitude', 'longitude'})).columns)
        n = len(A)
        for a in range(len(A)):
            l.append("'{}'".format(A[a] + ", " + A[a]))
        
        k = separator.join(l)

        covid3 = spark.sql(f'''

        SELECT * , stack({n},{k}) as data
        FROM covid2

        ''')

        covid3 = covid3.withColumn('data', split(col('data'),",")[0])

        A = list(covid3.columns)
        B = covid3.select(col('data')).collect()
        C = covid3.collect()
        l = []

        for b in range(covid3.count()):
            for a in range(len(A)):
                if B[b][0] == A[a]:
                    l.append("{}".format(C[b][a]))

        sparkContext = spark.sparkContext 
            
        rdd1 = sparkContext.parallelize(l)

        row_rdd = rdd1.map(lambda x: Row(x))

        windowSpec = Window.orderBy("id")

        df = spark.createDataFrame(row_rdd,[f'{type}']) \
            .withColumn("id", monotonically_increasing_id()) \
            .withColumn("index", row_number().over(windowSpec))

        raw_df = covid3.withColumn("id", monotonically_increasing_id()) \
            .withColumn("index", row_number().over(windowSpec)) \
            .join(df, "index", "inner") \
            .select('pais','estado',col('latitude').cast('double'),col('longitude').cast('double'),to_timestamp(col('data'), 'M/d/yy').alias('data'), regexp_replace(f'{type}',"'","").cast("long").alias(f'{type}')) \
            .groupBy('pais','estado','latitude','longitude', 'data') \
            .agg(sum(f'{type}').alias(f'{type}'))

        raw_df.coalesce(1) \
            .withColumn('year', year(col('data'))) \
            .withColumn('month', month(col('data'))) \
            .write \
            .partitionBy('year', 'month') \
            .mode('overwrite') \
            .parquet(f'/home/airflow/datalake/raw/covid19/{repartition}')

        return

    def covid_trusted_pipeline():

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import year, col, lit
        from pyspark.sql import Row
        from pyspark.sql.window import Window

        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("airflow_app") \
            .config('spark.executor.memory', '6g') \
            .config('spark.driver.memory', '6g') \
            .config("spark.driver.maxResultSize", "1048MB") \
            .config("spark.port.maxRetries", "100") \
            .getOrCreate()

        raw_confirmados = spark.read.parquet('/home/airflow/datalake/raw/covid19/covid_confirmados/')

        raw_confirmados = raw_confirmados.withColumn('quantidade_mortes', lit(None)) \
            .withColumn('quantidade_recuperados', lit(None))

        raw_mortes = spark.read.parquet('/home/airflow/datalake/raw/covid19/covid_mortes/')

        raw_mortes = raw_mortes.withColumn('quantidade_confirmados', lit(None)) \
            .withColumn('quantidade_recuperados', lit(None))

        raw_recuperados = spark.read.parquet('/home/airflow/datalake/raw/covid19/covid_recuperados/')

        raw_recuperados = raw_recuperados.withColumn('quantidade_confirmados', lit(None)) \
             .withColumn('quantidade_mortes', lit(None))

        raw_df = raw_confirmados.unionByName(raw_mortes) \
             .unionByName(raw_recuperados)

        raw_df.registerTempTable('raw_df')

        trs_df = spark.sql('''

        SELECT pais
            , estado
            , latitude
            , longitude
            , data
            , SUM(quantidade_confirmados) AS quantidade_confirmados
            , SUM(quantidade_mortes) AS quantidade_mortes
            , SUM(quantidade_recuperados) AS quantidade_recuperados
        FROM raw_df
        GROUP BY 1,2,3,4,5
        ORDER BY 1,2,3,4,5

        ''' )

        trs_df.coalesce(1) \
            .withColumn('year', year(col('data'))) \
            .write \
            .partitionBy('year') \
            .mode('overwrite') \
            .parquet('/home/airflow/datalake/trusted/covid19/')

        return

    def covid_refined_pipeline():

        from pyspark.sql import SparkSession
        from pyspark.sql.functions import avg, year, col
        from pyspark.sql import Row
        from pyspark.sql.window import Window

        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("airflow_app") \
            .config('spark.executor.memory', '6g') \
            .config('spark.driver.memory', '6g') \
            .config("spark.driver.maxResultSize", "1048MB") \
            .config("spark.port.maxRetries", "100") \
            .getOrCreate()

        trs_df = spark.read.parquet('/home/airflow/datalake/trusted/covid19/')

        trs_df.createOrReplaceTempView("trs_df")

        refined_df = spark.sql("""

        SELECT pais
            , data

            , AVG(quantidade_confirmados) OVER ( PARTITION BY pais
                ORDER BY data 
                RANGE BETWEEN INTERVAL 6 days PRECEDING AND CURRENT ROW) AS media_movel_confirmados 

            , AVG(quantidade_mortes) OVER ( PARTITION BY pais
                ORDER BY data 
                RANGE BETWEEN INTERVAL 6 days PRECEDING AND CURRENT ROW) AS media_movel_mortes 

            , AVG(quantidade_recuperados) OVER ( PARTITION BY pais
                ORDER BY data 
                RANGE BETWEEN INTERVAL 6 days PRECEDING AND CURRENT ROW) AS media_movel_recuperados 

        FROM trs_df

        ORDER BY pais, data

        """)

        refined_df.coalesce(1) \
            .withColumn('year', year(col('data'))) \
            .write \
            .partitionBy('year') \
            .mode('overwrite') \
            .parquet('/home/airflow/datalake/refined/covid19/')

        return

    start = DummyOperator(task_id='start')

    t01 = PythonOperator(
        task_id='raw_confirmados',
        python_callable=covid_raw_pipeline,
        op_kwargs={
			'file':'time_series_covid19_confirmed_global.csv',
			'type':'quantidade_confirmados',
          'repartition':'covid_confirmados',
			'previous_date':'{{ ds }}',
			'execution_date':'{{ next_ds }}'
			},    
        )

    t02 = PythonOperator(
        task_id='raw_mortes',
        python_callable=covid_raw_pipeline,
        op_kwargs={
			'file':'time_series_covid19_deaths_global.csv',
			'type':'quantidade_mortes',
          'repartition':'covid_mortes',
			'previous_date':'{{ ds }}',
			'execution_date':'{{ next_ds }}'
			},    
        )

    t03 = PythonOperator(
        task_id='raw_recuperados',
        python_callable=covid_raw_pipeline,
        op_kwargs={
			'file':'time_series_covid19_recovered_global.csv',
			'type':'quantidade_recuperados',
          'repartition':'covid_recuperados',
			'previous_date':'{{ ds }}',
			'execution_date':'{{ next_ds }}'
			},    
        )

    t04 = PythonOperator(
        task_id='covid_trusted',
        python_callable=covid_trusted_pipeline    
    )

    t05 = PythonOperator(
        task_id='covid_refined',
        python_callable=covid_refined_pipeline    
    )

    end = DummyOperator(task_id='end')

    start >> [t01,t02,t03] >> t04 >> t05 >> end


