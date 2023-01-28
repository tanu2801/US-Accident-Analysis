
""" This Python module describes a Spark ETL job definition
# that implements best practices for ETL jobs. It can be
# submitted to a Spark cluster (or locally) using the 'spark-submit '
# command found in the '/bin' directory of all Spark distributions
Run with:
  ./bin/spark-submit --driver-memory 2g --executor-memory 2g src/main/main.py
"""
# Initialize findspark
import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from python.task import *
import logging
import re


s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)

def main():


    ### Locally running application
    spark_session = SparkSession.builder.master("local") \
        .appName("US Crash Analysis") \
        .getOrCreate()

    sc = spark_session.sparkContext
    sc.setLogLevel('ERROR')

    #use path = "../../resrouces" for running application without spark-submit
    path_to_csv = "resources/"

    damage_df = spark_session.read.option("header", True) \
        .csv(path_to_csv + "Damages_use.csv") \
        .distinct()

    person_df = spark_session.read.option("header", True) \
        .csv(path_to_csv + "Primary_Person_use.csv") \
        .distinct()

    units_df = spark_session.read.option("header", True) \
        .csv(path_to_csv + "Units_use.csv") \
        .distinct()

    charges_df = spark_session.read.option("header", True) \
        .csv(path_to_csv + "Charges_use.csv") \
        .distinct()

    male_kill_obj = MaleKilled(person_df)
    person_male_killed_df = male_kill_obj.kill()
    print("Number of crashes in which number of persons killed are male: ", person_male_killed_df.count())

    two_wheeler_obj = TwoWheeler(units_df)
    two_wheeler_df = two_wheeler_obj.two_wheel()
    print("Number of two wheelers booked for crashes: ", two_wheeler_df.count())

    state_obj = State(person_df)
    highest_crash_state_df = state_obj.highest_state()
    print("State with highest number of accidents in which females are involved: ", highest_crash_state_df.first()[0])

    injury_cnt_obj = VehicleMaker(units_df)
    unit_maker_df = injury_cnt_obj.unit_maker()
    print("Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death are: ")
    unit_maker_df.show()

    ethnic_user_obj = EthnicUser(units_df, person_df)
    up_top_ethnic_user_df = ethnic_user_obj.up_top_ethnic_user()
    print("Top ethnic user group of each unique body style are: ")
    up_top_ethnic_user_df.show()

    units_insurance_obj = Damage(units_df, damage_df)
    units_damage_insurance_df = units_insurance_obj.units_damage_insurance()
    print("Count of Distinct Crash IDs with No Damaged Property and Damage Level is above 4 and car avails Insurance: ",
          units_damage_insurance_df.first()[0])

    zip_code_obj = ZipCode(units_df, person_df)
    units_person_zip_crash_df = zip_code_obj.units_person_zip_crash()
    print("Top 5 Zip Codes with highest number of crashes with alcohols as the contributing factor: ")
    units_person_zip_crash_df.select(col("cleaned_zip_code")).show()

    charges_obj = Charge(charges_df, person_df, units_df)
    top_units_maker_df = charges_obj.top_units_maker()
    print("Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers: ")
    top_units_maker_df.show()

    spark_session.stop()


if __name__ == "__main__":
    print("Starting Application...")
    main()