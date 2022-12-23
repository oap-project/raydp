from os.path import dirname, realpath

import numpy as np
from pyspark.sql.functions import hour, quarter, month, year, dayofweek, dayofmonth, weekofyear, col, lit, udf, abs as functions_abs

# change this to where the dataset is
NYC_TRAIN_CSV = "file://" + dirname(realpath(__file__)) + "/fake_nyctaxi.csv"

def clean_up(data):
    data = data.filter(col("pickup_longitude")<=-72) \
            .filter(col("pickup_longitude")>=-76) \
            .filter(col("dropoff_longitude")<=-72) \
            .filter(col("dropoff_longitude")>=-76) \
            .filter(col("pickup_latitude")<=42) \
            .filter(col("pickup_latitude")>=38) \
            .filter(col("dropoff_latitude")<=42) \
            .filter(col("dropoff_latitude")>=38) \
            .filter(col("passenger_count")<=6) \
            .filter(col("passenger_count")>=1) \
            .filter(col("fare_amount") > 0) \
            .filter(col("fare_amount") < 250) \
            .filter(col("dropoff_longitude") != col("pickup_longitude")) \
            .filter(col("dropoff_latitude") != col("pickup_latitude"))
    return data

# Add time related features
def add_time_features(data):
    data = data.withColumn("day", dayofmonth(col("pickup_datetime")))
    data = data.withColumn("hour_of_day", hour(col("pickup_datetime")))
    data = data.withColumn("day_of_week", dayofweek(col("pickup_datetime"))-2)
    data = data.withColumn("week_of_year", weekofyear(col("pickup_datetime")))
    data = data.withColumn("month_of_year", month(col("pickup_datetime")))
    data = data.withColumn("quarter_of_year", quarter(col("pickup_datetime")))
    data = data.withColumn("year", year(col("pickup_datetime")))
    @udf("int")
    def night(hour, weekday):
        if ((16 <= hour <= 20) and (weekday < 5)):
            return int(1)
        else:
            return int(0)

    @udf("int")
    def late_night(hour):
        if ((hour <= 6) or (hour >= 20)):
            return int(1)
        else:
            return int(0)
    data = data.withColumn("night", night("hour_of_day", "day_of_week"))
    data = data.withColumn("late_night", late_night("hour_of_day"))
    return data

def add_distance_features(data):
    @udf("float")
    def manhattan(lat1, lon1, lat2, lon2):
        return float(np.abs(lat2 - lat1) + np.abs(lon2 - lon1))
    # Location of NYC downtown
    ny = (-74.0063889, 40.7141667)
    # Location of the three airport in NYC
    jfk = (-73.7822222222, 40.6441666667)
    ewr = (-74.175, 40.69)
    lgr = (-73.87, 40.77)
    # Features about the distance between pickup/dropoff and airport
    data = data.withColumn("abs_diff_longitude", functions_abs(col(
        "dropoff_longitude")-col("pickup_longitude"))) \
               .withColumn("abs_diff_latitude", functions_abs(col(
        "dropoff_latitude") - col("pickup_latitude")))
    data = data.withColumn("manhattan", col(
        "abs_diff_latitude")+col("abs_diff_longitude"))
    data = data.withColumn("pickup_distance_jfk", manhattan(
        "pickup_longitude", "pickup_latitude", lit(jfk[0]), lit(jfk[1])))
    data = data.withColumn("dropoff_distance_jfk", manhattan(
        "dropoff_longitude", "dropoff_latitude", lit(jfk[0]), lit(jfk[1])))
    data = data.withColumn("pickup_distance_ewr", manhattan(
        "pickup_longitude", "pickup_latitude", lit(ewr[0]), lit(ewr[1])))
    data = data.withColumn("dropoff_distance_ewr", manhattan(
        "dropoff_longitude", "dropoff_latitude", lit(ewr[0]), lit(ewr[1])))
    data = data.withColumn("pickup_distance_lgr", manhattan(
        "pickup_longitude", "pickup_latitude", lit(lgr[0]), lit(lgr[1])))
    data = data.withColumn("dropoff_distance_lgr", manhattan(
        "dropoff_longitude", "dropoff_latitude", lit(lgr[0]), lit(lgr[1])))
    data = data.withColumn("pickup_distance_downtown", manhattan(
        "pickup_longitude", "pickup_latitude", lit(ny[0]), lit(ny[1])))
    data = data.withColumn("dropoff_distance_downtown", manhattan(
        "dropoff_longitude", "dropoff_latitude", lit(ny[0]), lit(ny[1])))
    return data

def drop_col(data):
    data = data.drop("pickup_datetime") \
            .drop("pickup_longitude") \
            .drop("pickup_latitude") \
            .drop("dropoff_longitude") \
            .drop("dropoff_latitude") \
            .drop("passenger_count") \
            .drop("key")
    return data

def nyc_taxi_preprocess(data):
    data = clean_up(data)
    data = add_time_features(data)
    data = add_distance_features(data)
    return drop_col(data)

if __name__ == "__main__":
    import ray
    import raydp
    ray.init(address="local", num_cpus=6)
    spark = raydp.init_spark("NYCTAXI data processing",
                             num_executors=1,
                             executor_cores=1,
                             executor_memory="500M",
                             configs={"spark.shuffle.service.enabled": "true"})
    data = spark.read.format("csv") \
                     .option("header", "true") \
                     .option("inferSchema", "true") \
                     .load(NYC_TRAIN_CSV)
    # Set spark timezone for processing datetime
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Transform the dataset
    data = nyc_taxi_preprocess(data)
