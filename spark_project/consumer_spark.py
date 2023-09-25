import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import *
from pyspark import SparkConf
from pyspark.sql.functions import mean, min, max, col, count, round
from pyspark.sql.functions import concat, to_timestamp, lit
import sys


def inicijalizacija():
    conf = SparkConf()
    conf.setMaster("spark://spark-master:7077")
    conf.set("spark.cassandra.connection.host", "cassandra")
    conf.set("spark.cassandra.connection.port", "9042")
    spark = SparkSession.builder.config(conf=conf).appName("Projekat2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data_frame = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "vehicles_topic")
        .option("startingOffsets", "latest")
        .load()
    )
    schema = StructType(
        [
            StructField("latitude", StringType()),
            StructField("longitude", StringType()),
            StructField("speed_kmh", StringType()),
            StructField("id", StringType()),
            StructField("timestamp", StringType()),
            StructField("acceleration", StringType()),
            StructField("type", StringType()),
            StructField("distance", StringType()),
            StructField("odometer", StringType()),
            StructField("pos", StringType()),
        ]
    )
    parsed_values = data_frame.select(
        "timestamp", from_json(col("value").cast("string"), schema).alias("parsed_values")
    )
    df_org = parsed_values.selectExpr("timestamp", "parsed_values.latitude AS latitude",
                                      "parsed_values.longitude AS longitude",
                                      "parsed_values.speed_kmh AS speed_kmh",
                                      "parsed_values.id AS id",
                                      "parsed_values.acceleration AS acceleration",
                                      "parsed_values.type AS type",
                                      "parsed_values.distance AS distance",
                                      "parsed_values.odometer AS odometer",
                                      "parsed_values.pos AS pos")

    df_org = df_org.withColumn("pos", col("pos").cast("double"))
    df_org = df_org.withColumn("latitude", col("latitude").cast("double"))
    df_org = df_org.withColumn("longitude", col("longitude").cast("double"))

    df_org = df_org.filter(df_org.speed_kmh <= 120)
    return df_org


def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def vrati_statisticke_vrednosti(df, long1=None, long2=None, lat1=None, lat2=None):
    if (long1 is not None) & (long2 is not None) & (lat1 is not None) & (lat2 is not None):
        df_ret = df.where(
            (df.longitude < long1) &
            (df.longitude > long2) &
            (df.latitude < lat1) &
            (df.latitude > lat2)
        ).groupBy(window(df.timestamp, "10 seconds", "10 seconds")).agg(
            mean(df.speed_kmh).alias("mean_speed"),
            min(df.speed_kmh).alias("min_speed"),
            max(df.speed_kmh).alias("max_speed"),
            count(df.speed_kmh).alias("count_speed")
        )
        return df_ret
    else:
        return None


def top_n_lokacija(df, N=5, num_decimal_places=3):

    df_rounded_locations = df.select(round("latitude", num_decimal_places).alias("latitude"),
                                     round("longitude", num_decimal_places).alias("longitude"))

    df_with_window = df_rounded_locations \
        .groupBy(window(df.timestamp, "10 seconds", "10 seconds"), "latitude", "longitude")\
        .agg(count("latitude").alias("freq"))

    top_n_locations = df_with_window.orderBy("freq", ascending=False).limit(N)
    return top_n_locations


def upis_statistickih_vrednosti(writeDF, _):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="statistika", keyspace="locationsdb") \
        .save()
    writeDF.show()


def upis_top_n_lokacija(writeDF, _):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="top_locations", keyspace="locationsdb") \
        .save()
    writeDF.show()


def izvrsenje_po_sirini_i_duzini(df):
    long1, long2, lat1, lat2 = float(sys.argv[1]),\
        float(sys.argv[2]),\
        float(sys.argv[3]),\
        float(sys.argv[4])

    print("Statističke vrednosti za slučaj kada su prosleđene samo širina i dužina")
    df_statistika = vrati_statisticke_vrednosti(df, long1=long1, long2=long2, lat1=lat1, lat2=lat2)
    df_statistika_baza = df_statistika.selectExpr("window.start as start",
                                                  "window.end as end",
                                                  "mean_speed",
                                                  "min_speed",
                                                  "max_speed",
                                                  "count_speed")

    print("Pronalazak top N lokacija")
    df_top_n_locations = top_n_lokacija(df)
    df_top_n_locations_baza = df_top_n_locations.selectExpr("window.start as start",
                                                            "window.end as end",
                                                            "latitude",
                                                            "longitude",
                                                            "freq")

    query1 = (df_statistika_baza.writeStream
              .foreachBatch(upis_statistickih_vrednosti)
              .outputMode("update")
              .start())
    query2 = (df_top_n_locations_baza.writeStream
              .foreachBatch(upis_top_n_lokacija)
              .outputMode("complete")
              .start())
    query1.awaitTermination()
    query2.awaitTermination()


if __name__ == '__main__':

    brojArgumenata = len(sys.argv)
    if brojArgumenata < 2:
        print("Usage: main.py <input folder> ")
        exit(-1)
    if brojArgumenata != 5:
        print("Lose prosledjeni argumenti")
        exit(-1)
    if brojArgumenata == 5:
        if (isfloat(sys.argv[1])) & (isfloat(sys.argv[2])) & (isfloat(sys.argv[3])) & (isfloat(sys.argv[4])):
            df = inicijalizacija()
            izvrsenje_po_sirini_i_duzini(df)
        else:
            print("Lose prosledjeni argumenti")
            exit(-1)
    else:
        print("Lose prosledjeni argumenti")
        exit(-1)
