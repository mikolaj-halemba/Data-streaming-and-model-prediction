# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 app.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
)
import pickle
from pyspark.sql.functions import pandas_udf, PandasUDFType
from joblib import load
import pandas as pd
from pymongo import MongoClient
import json


if __name__ == "__main__":
    
    spark = SparkSession.builder.appName("stream").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Json message schema
    json_schema = StructType(
        [
            StructField("event_time", StringType()),
            StructField("volume_transaction", FloatType()),
            StructField("avg_value_transactions", FloatType()),
            StructField("max_value_transactions", FloatType()),
            StructField("min_value_transactions", FloatType()),
        ]
    )
    
    # topic subscription
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "appML") # TOPIC NAME
        .load()
    )
    
    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("timestamp").alias("proc_time"),
        f.col("json").getField("event_time").alias("event_time"),
        f.col("json").getField("volume_transaction").alias("volume_transaction"),
        f.col("json").getField("avg_value_transactions").alias("avg_value_transactions"),
        f.col("json").getField("max_value_transactions").alias("max_value_transactions"),
        f.col("json").getField("min_value_transactions").alias("min_value_transactions")
    )
    df_spark = (parsed.select("volume_transaction","avg_value_transactions",
                              "max_value_transactions","min_value_transactions"))
    
    
    # Read model
    clf_model = load('clf_model.pkl')

    @pandas_udf(returnType="double", functionType=PandasUDFType.SCALAR)
    def predict(volume_transaction, avg_value_transactions, max_value_transactions, min_value_transactions):
        # Prepare Dataframe
        df = pd.DataFrame({
        "volume_transaction": volume_transaction, 
        "avg_value_transactions": avg_value_transactions,
        "max_value_transactions": max_value_transactions,
        "min_value_transactions": min_value_transactions
        })
        
        # Return prediction
        return pd.Series(clf_model.predict(df))

    # Add new column with prediction
    df_spark = parsed.withColumn('prediction', predict("volume_transaction","avg_value_transactions","max_value_transactions","min_value_transactions"))
    
    def foreach_batch_function(df, epoch_id):
        # Transformation Dataframe to Json
        data = df.toJSON().map(lambda j: json.loads(j)).collect()

        # Read configs
        with open('config.json') as f:
            config_data = json.load(f)
        username = config_data['username']
        password = config_data['password']

        # Create MongoDB
        client = MongoClient("mongo", 27017, username=username, password=password)

        # Choose database
        db = client["outputML"]  # Define in Docker compose

        collection = db["myCollection"]
        for row in data:
            collection.insert_one(row)

        # Close DB connection
        client.close()

    # defining output
    query = (df_spark.writeStream.outputMode("append")
                    .foreachBatch(foreach_batch_function)
                    .start())
    query.awaitTermination() #60
    query.stop()

    


