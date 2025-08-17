
from contextlib import contextmanager
import clickhouse_connect
import streamlit as st
import pandas as pd
import altair as alt
from altair import datum

st.set_page_config(layout="wide")

@contextmanager
def get_ch_client():
    
    try:
        client = clickhouse_connect.get_client(
            host=st.secrets["CLICKHOUSE_HOST"],
            user=st.secrets["CLICKHOUSE_USER"],
            password=st.secrets["CLICKHOUSE_PASSWORD"],
            secure=True
        )
        yield client
    finally:
        if client:
            del client
def run_query(query):
    with get_ch_client() as client:
        return client.query(query)

st.title("Analyzing NYC Yellow Taxi Trip Trends 2025 (Jan-May 2025)")
st.markdown("by:  Fadel Ahmad F")

with st.container(border=False, width=500):
    st.markdown("""
    This dashboard is part of my self-development journey into Data Engineer, Data Analysis, and Data Scientist.
    It is created to explore patterns in New York City Yellow Taxi Trips. 
    The dataset covers period January-May 2025, sourced from the official NYC Taxi and Limousine Commision:
    NYC TLC Trip Record Data that you can find on link below:""")

    st.markdown("https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")

def overallDaysLineChart():

    result = run_query("SELECT pickup_days, avg(total_trip) as avg_trip_days from default.taxi_trip_daily group by pickup_days")
    df = pd.DataFrame(result.result_rows, columns=result.column_names).iloc[:151]

    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    df['pickup_days'] = pd.Categorical(df['pickup_days'], categories=days, ordered=True)

    df_sorted = df.sort_values('pickup_days')

    st.line_chart(df_sorted, x="pickup_days", y="avg_trip_days", x_label="days", y_label="Average Trip")



def sumDaysLineChart():

    result = run_query("SELECT MONTH(pickup_date) as month, pickup_days, sum(total_trip) total_trip_per_days from default.taxi_trip_daily group by month, pickup_days order by month ")
    df = pd.DataFrame(result.result_rows, columns=result.column_names).iloc[:35]

    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    df['pickup_days'] = pd.Categorical(df['pickup_days'], categories=days, ordered=True)
    df_sorted = df.sort_values(['month','pickup_days'])

    chart = alt.Chart(df_sorted).mark_line().encode(
    x=alt.X("pickup_days",title="days"),
    y=alt.Y("total_trip_per_days",title="Total Trip"),
    color=alt.Color("month:N", legend=alt.Legend(title="month"))
    
    ).properties(
        width=800
    )

    st.altair_chart(chart)


def weekLineChart():

    result = run_query("SELECT MONTH(pickup_date) as month, pickup_days, avg(total_trip) avg_trip_per_days from default.taxi_trip_daily group by month, pickup_days order by month ")
    df = pd.DataFrame(result.result_rows, columns=result.column_names).iloc[:35]

    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    df['pickup_days'] = pd.Categorical(df['pickup_days'], categories=days, ordered=True)
    df_sorted = df.sort_values(['month','pickup_days'])

    chart = alt.Chart(df_sorted).mark_line().encode(
    x=alt.X("pickup_days", title="days"),
    y=alt.Y("avg_trip_per_days",title="Average Trip"),
    color=alt.Color("month:N", legend=alt.Legend(title="month"))
    
    ).properties(
        width=800
    )

    st.altair_chart(chart)


def TrendTripLineChart():

    # for i in range(1,6):
    result = run_query("""SELECT pickup_date, total_trip, MONTH(pickup_date) AS month 
                          FROM default.taxi_trip_daily 
                          WHERE MONTH(pickup_date) < 6 
                          ORDER BY pickup_date ASC;""")
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    df['month'] = 'Month ' + df['month'].astype(str)
 
    chart = alt.Chart(df).mark_line().encode(
        x=alt.X('pickup_date:T', title='date'),
        y=alt.Y('total_trip:Q', title='Total Trip'),
        color=alt.Color('month:N', legend=alt.Legend(title="Months")),
        tooltip=['pickup_date:T', 'total_trip:Q', 'month:N']
    ).properties(
        title='Perbandingan Jumlah Trip per Bulan',
        width=800,
        height=400
    ).configure_axisX(
        labelAngle=45
    )


    st.altair_chart(chart)

    
# Verifikasi urutan
# st.write("""
# ## Verifikasi Urutan:
# Perhatikan bahwa untuk setiap bulan, hari sekarang harus terurut dari Monday ke Sunday
# """)




def avgFareHourly():

    result = run_query("SELECT * from default.taxi_trip_fare_f")
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    chart = alt.Chart(df).mark_bar().encode(
        x = alt.X("pickup_hour", title="Time of Day (0-24)"),
        y = alt.Y("avg_fare_per_km", title="Average Fare per Kilometers")
    ).properties(
        width = 800
    )

    st.altair_chart(chart)



def distanceTripHourly():

    result = run_query("SELECT * from default.taxi_trip_fare_f")
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    chart = alt.Chart(df).transform_fold(
        ["short_trip_count","long_trip_count"],
        as_= ['metric','value']
    ).mark_line().encode(
        x = alt.X("pickup_hour:N",title="Time of Day (0-24)"),
        y = alt.Y("value:Q", title="Total Trip"),
        color='metric:N'
    ).properties(
        width = 800
    )

    st.altair_chart(chart)


def avgTipHourly():
    result = run_query("SELECT pickup_hour,avg_tip_amount from default.taxi_trip_fare_f")
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    chart = alt.Chart(df).mark_bar().encode(
        x = alt.X("pickup_hour", title="Time of Day (0-24)"),
        y = alt.Y("avg_tip_amount",title="Average Tips")
    ).properties(
        width = 800
    )

    st.altair_chart(chart)


def durationTripDistribution():
    result = run_query(""" SELECT 
	duration_interval,
    duration_interval_class, 
    sum(trip_count) as trip_count 
    FROM default.taxi_duration_interval_f 
    GROUP BY duration_interval,duration_interval_class
    ORDER BY duration_interval """)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    df['interval_start'] = df['duration_interval_class'].str.extract(r'^(\d+\.?\d*)').astype(float)
    df = df.sort_values('interval_start')

    # Buat daftar urutan kategori yang benar
    category_order = df['duration_interval_class'].tolist()

    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('duration_interval_class:N', 
               title='Duration Interval (minutes)',
               sort=category_order), 
        y=alt.Y('trip_count:Q', title='Number of Trips'),
        tooltip=['duration_interval_class', 'trip_count']
    ).properties(
        title='Distribution of Trip Durations',
        width=800
    )
    
    st.altair_chart(chart)



def mostRouteTrip():
    query = """ 
    WITH mostTripZone AS (
    select * from `pick_drop_zone_f` p 
    order by trip_count desc
    LIMIT 10
    )
    SELECT 
    t1.`Zone` as pickup_zone, 
    t2.`Zone` as drop_zone,
    `trip_count` ,
    round(`avg_trip_distance_km`,2) as avg_trip_distance_km , 
    round(`avg_fare_amount` ,2) as avg_fare_amount
    from mostTripZone
    left join `taxi_zone_lookup` t1 
    on PULocationID = t1.LocationID
    left join `taxi_zone_lookup` t2
    on DOLocationID = t2.LocationID"""

    result = run_query(query)

    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    st.dataframe(df)



  
def showChart():

    # col1,col2,col3 = st.columns(3)
    # with st.container(border = True, horizontal_alignment="center"):
        
    #     with st.container(width = 800, horizontal = True, horizontal_alignment="center"):
    #         overallDaysLineChart()
    with st.container(border = True):
        with st.container(border = True, horizontal = True, width = 1500):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader("a. Overall Average Daily Taxi Trips (Jan-May 2025)")
                overallDaysLineChart()
            with col2:
                st.subheader("b. Average Daily Taxi Trips by Weekday and Month (Jan-May 2025)")
                weekLineChart()
            with col3:
                st.subheader("c. Total Daily Taxi Trips by Weekday and Month (Jan-May 2025)")
                sumDaysLineChart()
        with st.container(border = False, horizontal=True,horizontal_alignment="center"):
            col1, col2, col3 = st.columns(3)
            with col1:
                with st.container(border = True, horizontal = True, width = 1500):
                    st.write("""
                            (a)  The line chart displays overall taxi trips by weekday from January to May 2025. Overall, taxi trips gradually increased from Monday to peaking on Thursday. It then significantly drop on Sunday.
                            """)
            with col2:
                with st.container(border = True, horizontal = True, width = 1500):
                    st.write("""
                            (b) 
                                This line chart illustrates average of taxi trips by weekday for each months. All months had similar patterns following to chart (a). However, it steadly	increased from January to May 2025.
                            """)
            with col3:    
                with st.container(border = True, horizontal = True, width = 1500):    
                    st.write("""
                            (c) Taxi trips typically rose from Monday, peaked on Thursday and declined on Sunday. Despite slight variations across months, the downward trend on Sunday remained consistent.
                            """)
                    
    with st.container(border = True):
        st.subheader("d. Average Fare per Kilometer by Hour of Day (Jan-May 2025)")
        avgFareHourly()
        st.write("""
                   (d) The bar chart shows fluctuations in average per-kilometers fare throughout the day. However, fare were lowest in the morning and at(7-8 AM) at under 10 dollars, likely due to public transportation competition. While the highest were at late-night, reach $16 due to limited supply.

                 """
        )
        
        st.subheader("e. Total Long vs Short Taxi Trips by Hour of Day (Jan-May 2025)")
        distanceTripHourly()
        st.write("""
                    (e) The chart shows trend of short and long trip. Both declined when came to midnight until late-night, but turned back at 6am. Additionally,  short trips dominated throughout the day, peaking at 6pm.  

                """)
        
        st.subheader("f.")
        avgTipHourly()
        st.write("""
                         
                    (f) The bar charts displays the distribution of average tips given by passengers. It fluctuated over hours. However the highes tips given were at 5pm. 
                         
                         """)

                                    
    
    with st.container(border = True):
          st.subheader("g. Average Trip by Hour of Day (Jan-May 2025) ")
          TrendTripLineChart()
          st.write("The line chart shows numbers of trip trend from January until May 2025. It steadily increased with fluctuations from below 80,000 to  around 120,000 trips.")
  

    with st.container(border = True):

        col1,col2  = st.columns(2)

        with col1:
          st.subheader("h. Distribution of Trip Duration")    
          durationTripDistribution()    
        with col2:
          st.subheader("i. Top Ten Yellow Taxi Routes (Jan-May 2025)")
          mostRouteTrip()

showChart()

# Data Engineer

st.header("The Data Pipeline")
st.markdown("""This dashboard was created for my self-practice while following short courses on Data Engineer Bootcamp.
                Here is the data flow and tech stack I use in this project:
            """)

with st.container(border=True):
    st.image("tech-stack.png", caption="data flow and tech stack")

    st.markdown("Source file")
    st.markdown("Source data were from link I write above. Data files were downloaded manually each months. Format of the files is parquet.")

    st.markdown("PySpark")
    st.markdown("I used pyspark to extract and transform data before I store it to postgresql.")

    st.markdown("Postgresql")
    st.markdown("I store the extracted and transformed data into postgresql before I send it to Clickhouse.")

    st.markdown("Kafka")
    st.markdown("Kafka was used as message broker to send data from postgresql into Clickhouse.") 

    st.markdown("Clickhouse")
    st.markdown("I used Clickhouse to store the data that ready to being presented into dashboard by streamlit.")   

    st.markdown("Streamlit")
    st.markdown("Streamlit was used to build dashboard for data analysis and data visualization")

    st.markdown("Docker")
    st.markdown("I used docker to run postgresql, clickhouse, and kafka locally")

    st.markdown("Vega-Altair")
    st.markdown("Vega-altair used for data visualization.")


st.title("The Journey")
st.write("""I am going to tell you about the steps I have done.
            Note that I may not tell you everything in detail, you can find it all by yourself if you have an intention to build your own project by following mine
         """)

with st.container(border=True):
    st.subheader("Install and Running Docker")
    st.write("You can install Docker first, you also may use docker dekstop to run locally")
    st.write("")
    st.write("To use postgresql, I set my docker postgresql image environment")
    code = """
            $ docker run -d 
            --name some-postgres 
            -e POSTGRES_PASSWORD=mysecretpassword 
            -e PGDATA=/var/lib/postgresql/data/pgdata 
           """
    st.code(code)
    st.write("then:")
    code = """ configure your postgres wal (Write Ahead Log) by set into 'logical', the path of postgres.conf file on your docker at path:
        
        /var/lib/postgresql/data/postgresql.conf
           """
    st.code(code)

    st.write("I setting my docker Kafka image by making file docker-compose.yml")
    code = """

            services:
            zookeeper:
                image: confluentinc/cp-zookeeper:7.2.15
                environment:
                ZOOKEEPER_CLIENT_PORT: 2181
                ports:
                - "2181:2181"
                networks:
                - data_pipeline_nyc ## build your own docker network

            kafka:
                image: confluentinc/cp-kafka:7.2.15
                depends_on:
                - zookeeper
                ports:
                - "9092:9092"
                - "29092:29092"
                environment:
                KAFKA_BROKER_ID: 1
                KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
                KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
                KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
                KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
                networks:
                - data_pipeline_nyc ## build your own docker network

            debezium:
                image: debezium/connect:2.7.0.Final

                environment:
                BOOTSTRAP_SERVERS: kafka:9092
                GROUP_ID: 1
                CONFIG_STORAGE_TOPIC: connect_configs
                OFFSET_STORAGE_TOPIC: connect_offsets
                # KEY_CONVERTER: io.confluent.connect.avro.AvroConverter
                # VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
                CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
                CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081
                depends_on: [kafka]
                ports:
                - 8083:8083
                networks:
                - data_pipeline_nyc ## build your own docker network
            
            schema-registry:
                image: confluentinc/cp-schema-registry:7.2.15
                environment:
                - SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:2181
                - SCHEMA_REGISTRY_HOST_NAME=schema-registry
                - SCHEMA_REGISTRY_LISTENERS=http://schema-registry:8081,http://localhost:8081
                - SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9092
                - SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=PLAINTEXT      
                ports:
                - 8081:8081
                depends_on: [zookeeper, kafka]
                networks:
                - data_pipeline_nyc ## build your own docker network
            
            kafka_manager:
                image: hlebalbau/kafka-manager:stable
                restart: always
                ports:
                - "9000:9000"
                depends_on:
                - zookeeper
                - kafka
                environment:
                ZK_HOSTS: "zookeeper:2181"
                APPLICATION_SECRET: "random-secret"
                command: -Dpidfile.path=/dev/null
                networks:
                - data_pipeline_nyc ## build your own docker network
            
            networks:
            data_pipeline_nyc:  ## build your own docker network
                external: true

           """
    st.code(code)
    st.write( "Then I run docker-compose up on my terminal to run this yml file")
    st.code("docker-compose up")
    st.write("Setting Clickhouse docker by pulling clickhouse/clickhouse-server:latest image ")
    code = """docker run -d -p 18123:8123 -p 19000:9000 -e 
          CLICKHOUSE_PASSWORD= write-yours --name write-yours --ulimit 
          nofile=262144:262144 clickhouse/clickhouse-server"""

    st.code(code)

    st.write("Create dockers network to connect with other containers")
    code = """
            docker network create my-network-name # create network
            docker network connect my-network-name my-container-name # connect your container to your network, if you use 3 container, then connect it all to the network
           """
    st.code(code)

with st.container(border=True):
    st.subheader("Using PySpark: Extract, Transform, and Load")
    st.markdown("note that Windows, MacOs, or Linux have different configuration setting for running spark. In this case, I use MacOs. ")
    st.write("I am using JupyterLab for ETL using PySpark")
    st.write("Create Spark App")
    code = """
        from pyspark.sql import SparkSession
        import os
        import pandas as pd
        from pyspark.sql import functions as sf

        spark = SparkSession.builder 
        .appName(" createYourAppName ") 
        .config("spark.jars", "path/your_path/postgresql-42.7.3.jar")
        .getOrCreate()
        """
    st.code(code)

    st.write("Create Data Transformation Function")
    code = """
        # create new columns of total amount exlcuding tips 
        def exclude_tips(val):
          return val.withColumn("total_amount_excluding_tips", sf.round(sf.col("total_amount") -  sf.col("tip_amount"),2))

        # create new columns of only date
        def pickup_date(val):
            return val.withColumn("pickup_date", sf.to_date(sf.col("tpep_pickup_datetime")))

        # create new columns to know the day of the date
        def pickup_days(val):
            return val.withColumn("pickup_days", sf.date_format(sf.col("tpep_pickup_datetime"),"EEEE"))

        # create new column to only show the time (hour) of pickup time 
        def pickup_hour(val):
            return val.withColumn("pickup_hour", sf.hour(sf.col("tpep_pickup_datetime")))

        
        # create new column and only show the time (minute) of pickup time 
        def pickup_minute(val):
            return val.withColumn("pickup_minute", sf.minute(sf.col("tpep_pickup_datetime")))

        # create new column and convert miles to kilometers
        def convert_trip_to_km(val):
            return val.withColumn("trip_distance_km", sf.round(sf.col("trip_distance")*1.60934,2))

         # create new column  to show duration of a trip
        def trip_duration_sec(val):
            return val.withColumn("trip_duration_sec",sf.unix_timestamp("tpep_dropoff_datetime") - sf.unix_timestamp("tpep_pickup_datetime"))

        # drop missing values
        def drop_null_values(val):
            return val.na.drop() 
        
        # handle negative values
        def exclude_negative_total_amount(val):
            return val.filter(sf.col("total_amount") >= 0)
           """
    st.code(code)
    
    st.write("""Create function to run transformation function once at a time """)
    code = """
            def transform_data(taxi_data): # here I name the parameter taxi_data
                return (
                    taxi_data.transform(exclude_tips)
                    .transform(pickup_date)
                    .transform(pickup_days)
                    .transform(pickup_hour)
                    .transform(pickup_minute)
                    .transform(convert_trip_to_km)
                    .transform(trip_duration_sec)
                    .transform(exclude_negative_total_amount)
                    .transform(drop_null_values)
                        )
           """

    st.code(code)

    st.write("""set database url dan properties""")
    code = """
                url = "jdbc:postgresql://localhost:5432/your_database_name"
                props = {
                    "user": "your user",
                    "password": "your password",
                    "driver": "org.postgresql.Driver"
                }
           """
    
    st.code(code)

    st.write(" Executing the extracting, transforming, and loading data to postgresql with this function")
    code = """"
            def create_database():
    
                    for i in range(1,6):

                        # Extract Data
                        taxi_data = spark.read.parquet(f"./2025/yellow_tripdata_2025-0{i}.parquet")
                        table = f"taxi_trips_0{i}_2025"

                        # Transform Data
                        taxi_data = transform_data(taxi_data) 

                        # Load data to Postgresql
                        taxi_data.write.jdbc(url=url, table=table, mode="overwrite", properties= props)
                        print("create table success")

                    # also load taxi_zone_lookup for mapping your locationID to zone/location description
                    # can be downloaded in NYC taxi database source link
                    taxi_zone =  spark.read.csv("taxi_zone_lookup.csv", header=True)
                    taxi_zone.write.jdbc(url=url, table="taxi_zone", mode="overwrite", properties=props)
                    print("create table success")
           """
    st.code(code)

    st.write(""" I try to create Materealized Value Table by getting back data that already created in postgresql and create another table 
                for analysis or visualization. Therefore I can get data that not too raw for analytical purposes
             """
             )
    
    code = """
                def mv_aggregation():
                    try:
                        for i in range(1,6):
                            query = f\"\"\" ( select 
                                            pickup_date,
                                            pickup_hour,
                                            pickup_days,
                                            count(*) as count_trip
                                            from taxi_trips_0{i}_2025
                                            where date_part('year', pickup_date) = 2025
                                            group by pickup_date,pickup_hour,pickup_days ) AS agg_table
                                    \"\"\"
                            agg_table = spark.read.jdbc(url=url, table=query, properties=props)
                            agg_table.write.jdbc(url=url, table="temp_table", mode="append", properties=props)
                            print("create_table temp_table success")
                    
                        for i in range (1,6):
                            query = f\"\"\" ( 	
                                    select  
                                        floor(trip_duration_sec /60 / 5) * 5 as duration_interval,
                                        count(*) as trip_count
                                    from taxi_trips_0{i}_2025 t 
                                    where t.trip_duration_sec  > 0 
                                    group by duration_interval
                                    )                
                                    \"\"\"
                            
                            agg_table = spark.read.jdbc(url=url, table=query, properties=props)
                        
                            agg_table = agg_table.withColumn(
                                                "duration_interval_class",
                                                sf.when(
                                                    sf.col("duration_interval") > 180, ">180"
                                                ).otherwise(
                                                    sf.concat(
                                                        sf.col("duration_interval").cast("string"),
                                                        sf.lit("-"),
                                                        (sf.col("duration_interval") + 5).cast("string")
                                                    )
                                                )
                                            )
                            agg_table.write.jdbc(url=url, table="temp_duration_interval", mode="append", properties=props)
                            print("create table temp_duration_interval success")
                    
                        for i in range(1,6):
                            query = f\"\"\"
                                ( 
                                        WITH  DropPickZone as (
                                            select  t."PULocationID", t1."Zone" as pickup_zone, t."DOLocationID", t2."Zone" as drop_zone , t."trip_distance_km", t."total_amount_excluding_tips"
                                            from taxi_trips_0{i}_2025 t
                                            left join taxi_zone t1 
                                            on "PULocationID" = t1."LocationID"::integer 
                                            left join taxi_zone t2 
                                            on "DOLocationID" = t2."LocationID"::integer
                                            where t.total_amount_excluding_tips > 0
                                            ) 
                                        select
                                            "PULocationID",
                                            "DOLocationID",
                                            pickup_zone,
                                            drop_zone,
                                            count(*) as total_trip,
                                            AVG(trip_distance_km) as avg_trip_distance_km,
                                            avg(total_amount_excluding_tips) as avg_fare_amount
                                        from DropPickZone
                                        group by 	"PULocationID", "DOLocationID", pickup_zone, drop_zone )
                                \"\"\"
                            
                            agg_table = spark.read.jdbc(url=url, table=query, properties=props)
                            agg_table.write.jdbc(url=url, table="temp_pick_drop_zone", mode="append", properties=props)
                            print("create table temp_pick_drop_zone success")
                    
                        query = f\"\"\" (
                                        SELECT pickup_hour,
                                            AVG(total_amount_excluding_tips / NULLIF(trip_distance_km, 0)) AS avg_fare_per_km,
                                            AVG(tip_amount) AS avg_tip_amount,
                                            SUM(CASE WHEN trip_distance_km <= 3 THEN 1 ELSE 0 END) AS short_trip_count,
                                            SUM(CASE WHEN trip_distance_km > 3 THEN 1 ELSE 0 END) AS long_trip_count
                                        FROM (
                                                SELECT pickup_hour, total_amount_excluding_tips, trip_distance_km, tip_amount
                                                FROM taxi_trips_01_2025
                                                UNION ALL
                                                SELECT pickup_hour, total_amount_excluding_tips, trip_distance_km, tip_amount
                                                FROM taxi_trips_02_2025
                                                UNION ALL
                                                SELECT pickup_hour, total_amount_excluding_tips, trip_distance_km, tip_amount
                                                FROM taxi_trips_03_2025
                                                UNION ALL
                                                SELECT pickup_hour, total_amount_excluding_tips, trip_distance_km, tip_amount
                                                FROM taxi_trips_04_2025
                                                UNION ALL
                                                SELECT pickup_hour, total_amount_excluding_tips, trip_distance_km, tip_amount
                                                FROM taxi_trips_05_2025
                                        ) t
                                        WHERE total_amount_excluding_tips > 0
                                        GROUP BY pickup_hour
                                        )                 
                                        \"\"\"
                        agg_table = spark.read.jdbc(url=url, table=query, properties=props)
                        agg_table.write.jdbc(url=url, table="temp_trip_fare", mode="overwrite", properties=props)
                        print("create table temp_trip_fare success")
                                    
                    except Exception as error:
                        print(f"error occurred: {error}")
                        """

    st.code(code)

    st.write("SQL Query for making table at PostgreSQL")
    code = """
            # sql create MV
 
            CREATE MATERIALIZED VIEW mv_taxi_date_hour_daily AS
            SELECT 
                pickup_date,
                pickup_days,
                sum(count_trip) as total_trip
                from temp_table
                group by pickup_date,pickup_days
                order by pickup_date asc;


            CREATE MATERIALIZED VIEW mv_taxi_date_hourly AS
            select 
                pickup_date,
                pickup_hour,
                count_trip
                from temp_table;


            CREATE MATERIALIZED VIEW mv_taxi_duration_trip AS
                select t.duration_interval,t.duration_interval_class , sum(trip_count) as trip_count from temp_duration_interval t 
                group by duration_interval, t.duration_interval_class 
                order by duration_interval asc;	


            CREATE MATERIALIZED VIEW mv_pick_drop_zone AS
                select "PULocationID", "DOLocationID", sum(total_trip), round(avg(avg_trip_distance_km),2) as avg_trip_distance_km, round(avg(avg_fare_amount),2) as avg_fare_amount 
                from temp_pick_drop_zone
                group by "PULocationID", "DOLocationID"
                order by "PULocationID" , "DOLocationID" ;

            CREATE MATERIALIZED VIEW mv_pick_drop_zone AS
                SELECT * from temp_trip_fare;
            """
    
    st.code(code)

    with st.container(border = True):
        st.subheader("Kafka: FLows Data into ClickHouse")
        st.write("Using postman or other platform to get and create connectors for Producers in Kafka")

        code =  """
                GET | http://localhost:8083/
                GET | http://localhost:8083/connectors to check connectors
                POST | http://localhost:8083/connectors to create connectors

                """
        st.code(code)
        st.write("Using this body for creating connectors")
        code = """
                    {
                    "name": " write your own connectors name",
                    "config": {
                        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                        "database.hostname": "your ppostgre database connection",
                        "database.port": "5432",
                        "database.user": "your user",
                        "database.password": "your password",
                        "database.dbname": "your database name",
                        "plugin.name": "pgoutput",
                        "database.server.name": "source",
                        "key.converter.schemas.enable": "false",
                        "value.converter.schemas.enable": "false",
                        "transforms": "unwrap",
                        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                        "table.include.list": "public.temp_duration_interval,public.temp_pick_drop_zone,public.temp_table,
                                               public.temp_trip_fare", ## this is filled with the table you use for sending the data (become your Kafka Topics)
                        "slot.name" : "taxi_data_slot", # fill this by creating your own slot name
                        "topic.prefix" : "etl" # fill this by creating  your own prefix topic
                    }
                }

               """
        st.code(code)

        st.write("When finish creating, then check from the API, we will see something like this: ")
        st.code("""
                    [
                    " your kafka connectors "
                                             ]
                """)

        st.write("After that, crete cluster in your kafka manager, I set my port at :9000")
        st.code("""
                http://localhost:9000/

                choose menu clusters -> add cluster

                Cluster Name = create your own cluster name
                cluster zookeeper hosts= Zookeeper:your port
                check True "Enable JMX Polling (...."
                check True "Poll consumer information....."

                then click Save
                """)

        st.write(""" Testing your Kafka """)
        code = """
                ### Check Your Topics

                from kafka import KafkaConsumer
                    bootstrap_servers = ['localhost:29092']
                    consumer = KafkaConsumer( bootstrap_servers=bootstrap_servers)
                    consumer.topics()

                
                ### Test Your Kafka by sending a message from Consume side:
                # CONSUMER
                from kafka import KafkaConsumer
                import json

                consumer = KafkaConsumer(
                    'etl.public.your-topik',        # change base on your topic
                    bootstrap_servers=['localhost:29092'],
                    auto_offset_reset='earliest',   
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  #  for JSON Data
                )


                for i, msg in enumerate(consumer):
                    print(msg.value)
                    if i >= 10:   
                        break
                        
                
                # if you want to test the Producers Side


                # PRODUCER
                 
                Import KafkaProducer from Kafka library
                from kafka import KafkaProducer

                bootstrap_servers = ['localhost:29092']
                
                topicName = 'your topic'
                
                producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
                producer.send(topicName, b'Testing Kafka')
                
                print("Message Sent")

                """
        st.code(code)

with st.container(border = True):
        st.subheader("Clickhouse: Get data from message broker kafka for visualizing it into streamlit")
        st.write("I run the query for database in ClickHouse using DreamBeaver")
        st.write("Create Table Stream, Materealized View, and Target Table")
        code = """
                ---------------------- Stream from temp_duration interval --------------------------------
                -- 1. Table Kafka (Table Stream)
                CREATE TABLE `default`.taxi_duration_interval (
                    duration_interval Float,
                    trip_count UInt64,
                    duration_interval_class String,
                ) ENGINE = Kafka
                SETTINGS
                    kafka_broker_list = 'kafka-kafka-1:9092',
                    kafka_topic_list = 'etl.public.temp_duration_interval', #change base on topic
                    kafka_group_name = 'clickhouse_consumer_2',
                    kafka_format = 'JSONEachRow';

                CREATE TABLE `default`.taxi_duration_interval_f (
                    duration_interval Float,
                    duration_interval_class String,
                    trip_count UInt64
                ) ENGINE = MergeTree()
                ORDER BY duration_interval asc;


                CREATE MATERIALIZED VIEW `default`.taxi_duration_interval_mv 
                TO `default`.taxi_duration_interval_f
                AS
                select t.duration_interval,t.duration_interval_class , sum(trip_count) as trip_count 
                from `default`.taxi_duration_interval t 
                group by duration_interval, t.duration_interval_class ;	

                ----------------------- Stream From pick_drop_zone ------------------------------
                -- 1. Table Kafka
                CREATE TABLE default.pick_drop_zone
                (
                PULocationID UInt32,
                DOLocationID UInt32,
                pickup_zone String,
                drop_zone String,
                total_trip UInt64,
                avg_trip_distance_km Float64, 
                avg_fare_amount Float64
                ) ENGINE = Kafka
                SETTINGS
                    kafka_broker_list = 'kafka-kafka-1:9092',
                    kafka_topic_list = 'etl.public.temp_pick_drop_zone',
                    kafka_group_name = 'clickhouse_consumer_3',
                    kafka_format = 'JSONEachRow';

                -- 2. Table MergeTree
                CREATE TABLE default.pick_drop_zone_f
                (
                PULocationID UInt32,
                DOLocationID UInt32,
                trip_count UInt64,
                avg_trip_distance_km Float64,
                avg_fare_amount Float64
                ) ENGINE = MergeTree()
                ORDER BY (PULocationID, DOLocationID);

                -- 3. MV
                CREATE MATERIALIZED VIEW default.pick_drop_zone_mv
                TO default.pick_drop_zone_f
                AS
                SELECT
                    PULocationID,
                    DOLocationID,
                    sum(total_trip) AS trip_count,

				--------- Stream from temp_trip_fare --------------

				-- 1. Table Kafka
				CREATE TABLE default.taxi_trip_fare
				(
				   pickup_hour UInt64,
				   avg_fare_per_km Float64,
				   avg_tip_amount Float64,
				   short_trip_count UInt64,
				   long_trip_count UInt64
				) ENGINE = Kafka
				SETTINGS
				    kafka_broker_list = 'kafka-kafka-1:9092',
				    kafka_topic_list = 'etl.public.temp_trip_fare',
				    kafka_group_name = 'clickhouse_consumer_4',
				    kafka_format = 'JSONEachRow';
				
				-- 2. Table MergeTree
				CREATE TABLE default.taxi_trip_fare_f
				(
				   pickup_hour UInt64,
				   avg_fare_per_km Float64,
				   avg_tip_amount Float64,
				   short_trip_count UInt64,
				   long_trip_count UInt64
				) ENGINE = MergeTree()
				ORDER BY pickup_hour;
				
				-- 3. MV
				CREATE MATERIALIZED VIEW default.taxi_trip_fare_mv
				TO default.taxi_trip_fare_f
				AS
				SELECT *
				FROM default.taxi_trip_fare;
				
				--------- Stream from temp_table --------------
				
				-- 1. Table Kafka
				CREATE TABLE default.taxi_trip_daily_hourly
				(
				pickup_date Date,
				pickup_hour UInt64,
				pickup_days	String,
				count_trip UInt64
				) ENGINE = Kafka
				SETTINGS
				    kafka_broker_list = 'kafka-kafka-1:9092',
				    kafka_topic_list = 'etl.public.temp_table',
				    kafka_group_name = 'clickhouse_consumer_5',
				    kafka_format = 'JSONEachRow';
				
				-- 2. Table MergeTree daily
				CREATE TABLE default.taxi_trip_daily
				(
				   pickup_date Date,
				   pickup_days String,
				   total_trip UInt64,
				) ENGINE = MergeTree()
				
				-- 3. MV
				CREATE MATERIALIZED VIEW default.taxi_trip_daily_mv
				TO default.taxi_trip_daily
				AS
				SELECT 
				     pickup_date,
				     pickup_days,
				     sum(count_trip) as total_trip
				     from taxi_trip_daily_hourly
				     group by pickup_date,pickup_days
				     order by pickup_date asc;
                """
        
        st.code(code)
