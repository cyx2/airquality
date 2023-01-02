# SPDX-FileCopyrightText: 2020 by Bryan Siepert, written for Adafruit Industries
#
# SPDX-License-Identifier: Unlicense
import time
import board
import adafruit_scd4x
import mysql.connector
import os
from datetime import datetime
from dotenv import load_dotenv
import sys
import pymongo

load_dotenv()

sensor = adafruit_scd4x.SCD4X(board.I2C())
sensor.start_periodic_measurement()

pscale_cxn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)
cur = pscale_cxn.cursor()
pscale_query = (
    "INSERT INTO Measurements "
    "(MeasuredAt, CO2, Temp, Humidity) "
    "VALUES (%(MeasuredAt)s, %(CO2)s, %(Temp)s, %(Humidity)s)"
)

mdb_client = client = pymongo.MongoClient(
    f"mongodb+srv://{os.getenv('MDB_USER')}:"
    f"{os.getenv('MDB_PASSWORD')}@"
    f"{os.getenv('MDB_HOST')}"
)
mdb_db = mdb_client[os.getenv("MDB_DB")]
mdb_coll = mdb_db[os.getenv("MDB_COLL")]


print("Starting airqual_measure.py")

while True:
    try:
        if sensor.data_ready:

            data_measurement = {
                "MeasuredAt": datetime.combine(
                    datetime.now().date(), datetime.now().time()
                ),
                "CO2": sensor.CO2,
                "Temp": sensor.temperature,
                "Humidity": sensor.relative_humidity,
            }

            cur.execute(pscale_query, data_measurement)
            pscale_cxn.commit()

            mdb_coll.insert_one(data_measurement)

            print("Data inserted.")

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nKeyboard Interrupt")

        pscale_cxn.close()
        print("Planetscale Connection closed.")

        mdb_client.close()
        print("MongoDB Connection closed.")

        sys.exit(1)
    except SystemExit:
        print("\nSystem Interrupt")

        pscale_cxn.close()
        print("Planetscale Connection closed.")

        mdb_client.close()
        print("MongoDB Connection closed.")

        sys.exit(1)
