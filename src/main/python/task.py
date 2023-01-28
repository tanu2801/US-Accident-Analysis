#!/usr/bin/env python
# coding: utf-8


# Initialize findspark
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
import re



####################### ANALYSIS -1 ###########################

class MaleKilled:

  def __init__(self, df):
    self.df = df

  #Function to find males killed in accident
  def kill(self):
    self.df1 = self.df.filter((col("PRSN_GNDR_ID") == 'MALE') & (col("PRSN_INJRY_SEV_ID") == 'KILLED'))
    return self.df1



####################### ANALYSIS -2 ###########################


class TwoWheeler:

  def __init__(self, df):
    self.df = df

  #Function to find two wheelers
  def two_wheel(self):
    self.df1 = self.df.filter((col("VEH_BODY_STYL_ID").isin(['MOTORCYCLE', 'POLICE MOTORCYCLE'])))
    return self.df1



####################### ANALYSIS -3 ###########################

class State:

  def __init__(self, df):
    self.df = df

    # Function to find female
  def female(self):
    self.df1 = self.df.filter((~col("DRVR_LIC_STATE_ID").isin(['NA', 'Unknown'])) & (col("PRSN_GNDR_ID") == 'FEMALE'))
    return self.df1

    # Function to find the highest state with female accidents
  def highest_state(self):
      self.df1 = self.female().groupBy(col("DRVR_LIC_STATE_ID")).agg(count(col("CRASH_ID")).alias("count_of_crash_id")) \
                         .orderBy(col("count_of_crash_id").desc()) \
                         .limit(1) \
                         .select(col("DRVR_LIC_STATE_ID"))
      return self.df1



####################### ANALYSIS -4 ###########################


class VehicleMaker:

  def __init__(self, df):
    self.df = df

    # Function to clean units df column
  def units_clean(self):
    self.df1 = self.df.filter(~col("VEH_MAKE_ID").isin(['NA', 'UNKNOWN'])).distinct()
    return self.df1

    # Function to find death count
  def injury_death_count(self):
      self.df1 = self.units_clean().select(col("CRASH_ID"), col("VEH_MAKE_ID"), col("TOT_INJRY_CNT"), col("DEATH_CNT")) \
                    .groupBy("VEH_MAKE_ID") \
                    .agg(sum(self.units_clean()["TOT_INJRY_CNT"] + self.units_clean()["DEATH_CNT"]).alias("total_injury_death_count")) \
                    .select(col("VEH_MAKE_ID"), col("total_injury_death_count"))
      return self.df1

  def unit_maker(self):
      df3 = self.injury_death_count().withColumn("row_num", row_number().over(Window.orderBy(col("total_injury_death_count").desc())))
      self.df1 = df3.filter(col("row_num").between(5, 15)).select(col("VEH_MAKE_ID").alias("Vehicle Maker"))
      return self.df1




######################## ANALYSIS -5 ###########################


class EthnicUser:

  def __init__(self, df1, df2):
    self.df1 = df1
    self.df2 = df2

    # Function to clean units df column
  def units_body_clean(self):
    self.df3 = self.df1.filter(~col("VEH_BODY_STYL_ID").isin(['NA', 'UNKNOWN'])) \
                        .distinct() \
                        .select(col("CRASH_ID"), col("VEH_BODY_STYL_ID"))
    return self.df3

  def person_ethnic_clean(self):
    self.df3 = self.df2.filter(~col("PRSN_ETHNICITY_ID").isin(['NA', 'UNKNOWN'])) \
                        .distinct() \
                        .select(col("CRASH_ID"), col("PRSN_ETHNICITY_ID"))
    return self.df3

  # Function to find death count
  def units_person_union(self):
      self.df3 = self.units_body_clean().alias("c").join(self.person_ethnic_clean().alias("d"),
                 self.units_body_clean()['CRASH_ID'] == self.person_ethnic_clean()['CRASH_ID'], "inner") \
                 .select("c.CRASH_ID", "c.VEH_BODY_STYL_ID", "d.PRSN_ETHNICITY_ID")
      return self.df3

  def up_ethnic_user_count(self):
      self.df3 = self.units_person_union().groupBy(col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID")) \
          .agg(count(col("PRSN_ETHNICITY_ID")).alias("total_count_of_ethnic_user")) \
          .orderBy(col("total_count_of_ethnic_user").desc())
      return self.df3

  def up_top_ethnic_user(self):
      window_spec = Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(col("total_count_of_ethnic_user").desc())

      df4 = self.up_ethnic_user_count().withColumn("rank_by_top_ethnic_user",dense_rank().over(window_spec))

      self.df3 = df4.filter(col("rank_by_top_ethnic_user") == 1) \
                    .select(col("VEH_BODY_STYL_ID").alias("vehicle body style"),
                    col("PRSN_ETHNICITY_ID").alias("top ethnic user group"))
      return self.df3



####################### ANALYSIS -6 ###########################

class ZipCode:

  def __init__(self, df1, df2):
    self.df1 = df1
    self.df2 = df2

    self.zip_code_clean_udf = udf(ZipCode.zip_code_clean, StringType())

    # Function to clean units df column
  def units_car_alcohol(self):
    self.df3 = self.df1.filter((col("VEH_BODY_STYL_ID").isin(["PASSENGER CAR, 2-DOOR", "PASSENGER CAR, 4-DOOR", "POLICE CAR/TRUCK"])) & (col("CONTRIB_FACTR_1_ID") == 'UNDER INFLUENCE - ALCOHOL')) \
               .select(col("CRASH_ID"), col("VEH_BODY_STYL_ID"))
    return self.df3

  @staticmethod
  def zip_code_clean(zp):
      zp = str(zp)
      if re.match(r"^\d{9}$", zp):
          new_zp = zp[:5] + "-" + zp[5:]
          return new_zp
      elif re.match(r"^\d{5}-\d{4}$", zp):
          return zp
      elif re.match(r"^\d{5}.*", zp):
          new_zp = zp[:5]
          return new_zp
      elif re.match(r"^\d{5}$", zp):
          return zp
      elif re.match(r"^\d{4}$", zp):
          return zp
      else:
          return 'invalid'

  def person_clean_zip(self):
    self.df3 = self.df2.withColumn("cleaned_zip_code", self.zip_code_clean_udf(self.df2['DRVR_ZIP'])) \
                      .drop(col("DRVR_ZIP"))
    return self.df3

  def units_person_zip_union(self):
      self.df3 = self.units_car_alcohol().alias("e").join(self.person_clean_zip().alias("f"),
                 self.units_car_alcohol()['CRASH_ID'] == self.person_clean_zip()['CRASH_ID'], "inner") \
                 .select("f.CRASH_ID", "e.VEH_BODY_STYL_ID", "f.cleaned_zip_code")
      return self.df3

  def units_person_zip_crash(self):
      self.df3 = self.units_person_zip_union().filter(col("cleaned_zip_code") != 'invalid')\
                            .groupBy(col("cleaned_zip_code")) \
                            .agg(count(col("CRASH_ID")).alias("total_count_of_crashes")) \
                            .orderBy(col("total_count_of_crashes").desc()) \
                            .limit(5)
      return self.df3


####################### ANALYSIS -7 ###########################

class Damage:
  def __init__(self, df1, df2):
    self.df1 = df1
    self.df2 = df2

  def units_insurance(self):
    self.df3 = self.df1.filter(
          (col("VEH_BODY_STYL_ID").isin(["PASSENGER CAR, 2-DOOR", "PASSENGER CAR, 4-DOOR", "POLICE CAR/TRUCK"]))
        & (col("VEH_DMAG_SCL_1_ID").isin(["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"]))
        & (col("VEH_DMAG_SCL_2_ID").isin(["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"]))
        & (~col("FIN_RESP_TYPE_ID").isin(["NA", "CERTIFICATE OF DEPOSIT WITH COMPTROLLER", "CERTIFICATE OF DEPOSIT WITH COUNTY JUDGE", "SURETY BOND"]))) \
        .distinct()
    return self.df3

  def units_damage_insurance(self):
    self.df3 = self.units_insurance().join(self.df2,
               self.units_insurance()['CRASH_ID'] == self.df2['CRASH_ID'], "leftanti") \
               .select(countDistinct("CRASH_ID").alias("distinct count of crashes"))
    return self.df3



####################### ANALYSIS -8 ###########################


class Charge:
  def __init__(self, df1, df2, df3):
    self.df1 = df1
    self.df2 = df2
    self.df3 = df3

  def charges_speed(self):
    self.df4 = self.df1.filter(col("CHARGE").rlike("(?i)^*SPEED[^.]*$")) \
                .select(col("CRASH_ID"), col("CHARGE"))
    return self.df4

  def person_licensed(self):
    self.df4 = self.df2.filter(~col("DRVR_LIC_CLS_ID").isin(['NA', 'UNKNOWN', "UNLICENSED"])) \
                           .select(col("CRASH_ID"), col("DRVR_LIC_CLS_ID"))
    return self.df4

  def units_state_color(self):
    self.df4 = self.df3.filter((col("VEH_BODY_STYL_ID") \
              .isin(["PASSENGER CAR, 2-DOOR", "PASSENGER CAR, 4-DOOR", "POLICE CAR/TRUCK"]))
              & (~col("VEH_COLOR_ID").isin(['NA', '98', "99"]))
              & (~col("VEH_LIC_STATE_ID").isin(['NA', '98']))
              & (~col("VEH_MAKE_ID").isin(['NA', 'UNKNOWN'])))   \
              .distinct() \
              .select(col("CRASH_ID"), col("VEH_BODY_STYL_ID"), col("VEH_COLOR_ID"), col("VEH_LIC_STATE_ID"), col("VEH_MAKE_ID"))
    return self.df4


  def units_person_charge_union(self):
      self.df4 = self.units_state_color().alias("h").join(self.charges_speed(), self.units_state_color()['CRASH_ID'] == self.charges_speed()['CRASH_ID'], "inner") \
                .join(self.person_licensed(), self.units_state_color()['CRASH_ID'] == self.person_licensed()['CRASH_ID'], "inner") \
                .select(col("h.CRASH_ID"), col("VEH_BODY_STYL_ID"), col("VEH_COLOR_ID"), col("VEH_LIC_STATE_ID"), col("VEH_MAKE_ID"), col("DRVR_LIC_CLS_ID"), col("CHARGE"))

      return self.df4


  def top_units_maker(self):
     self.df4 = self.units_person_charge_union().groupBy(col("VEH_MAKE_ID")) \
          .agg(count(col("CHARGE")).alias("total_count")) \
          .orderBy(col("total_count").desc()) \
          .select(col("VEH_MAKE_ID").alias("Vehicle maker")).limit(5)
     return self.df4

