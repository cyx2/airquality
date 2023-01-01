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

load_dotenv()

sensor = adafruit_scd4x.SCD4X(board.I2C())
sensor.start_periodic_measurement()

cnx = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

# Get a cursor
cur = cnx.cursor()

add_measurement = (
    "INSERT INTO Measurements "
    "(MeasuredAt, CO2, Temp, Humidity) "
    "VALUES (%(MeasuredAt)s, %(CO2)s, %(Temp)s, %(Humidity)s)"
)

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

            cur.execute(add_measurement, data_measurement)

            cnx.commit()

            print("Added data.")

            time.sleep(1)
    except KeyboardInterrupt:
        cnx.close()
        print("DB connection closed, keyboard interrupt.")
        sys.exit(1)
    except SystemExit:
        cnx.close()
        print("DB connection closed, system interrupt.")
        sys.exit(1)
