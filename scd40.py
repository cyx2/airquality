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

load_dotenv()

scd4x = adafruit_scd4x.SCD4X(board.I2C())
scd4x.start_periodic_measurement()

cnx = mysql.connector.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
    )

# Get a cursor
cur = cnx.cursor()

add_measurement = ("INSERT INTO Measurements "
                   "(MeasuredAt, CO2, Temp, Humidity) "
                   "VALUES (%(MeasuredAt)s, %(CO2)s, %(Temp)s, %(Humidity)s)")

while True:
    if scd4x.data_ready:

        data_measurement = {
            'MeasuredAt': datetime.combine(datetime.now().date(), datetime.now().time()),
            'CO2': scd4x.CO2,
            'Temp': scd4x.temperature,
            'Humidity': scd4x.relative_humidity,
        }

        cur.execute(add_measurement, data_measurement)

        cnx.commit()
    time.sleep(1)

