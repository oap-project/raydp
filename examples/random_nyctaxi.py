import os
import argparse

import numpy as np
import pandas as pd

base_date = np.datetime64("2010-01-01 00:00:00")

parser = argparse.ArgumentParser(description="Rabdin NYC taxi Generator")
parser.add_argument(
    "--num-records",
    type=int,
    default=2000,
    metavar="N",
    help="number of records to generate (default: 2000)")

args = parser.parse_args()

N = args.num_records

fare_amount = np.random.uniform(3.0, 50.0, size=N)
pick_long = np.random.uniform(-74.2, -73.8, size=N)
pick_lat = np.random.uniform(40.7, 40.8, size=N)
drop_long = np.random.uniform(-74.2, -73.8, size=N)
drop_lat = np.random.uniform(40.7, 40.8, size=N)
passenger_count = np.random.randint(1, 5, size=N)
date = np.random.randint(0, 157680000, size=N) + base_date
date = np.array([t.item().strftime("%Y-%m-%d %H:%m:%S UTC") for t in date])
key = ["fake_key"] * N
df = pd.DataFrame({
    "key": key,
    "fare_amount":fare_amount,
    "pickup_datetime": date,
    "pickup_longitude": pick_long,
    "pickup_latitude": pick_lat,
    "dropoff_longitude": drop_long,
    "dropoff_latitude": drop_lat,
    "passenger_count": passenger_count
    })
csv_path = os.path.dirname(os.path.realpath(__file__)) + "/fake_nyctaxi.csv"
df.to_csv(csv_path, index=False)
