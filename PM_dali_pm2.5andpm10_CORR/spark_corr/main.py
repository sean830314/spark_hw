#-*-coding:utf-8 -*-
#Spark Sql  2015 PM2.5 top10
import random
import os
os.chdir("C:\Python27")
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
if __name__ == "__main__":
    sc = SparkContext("local", "PythonWordCount")
    sqlContext = SQLContext(sc)
    weather_data = sc.textFile("C:/Users/ekko/Desktop/pm2.5Taiwan.csv", 1)
    weather_data_rdd = weather_data.map(lambda line: line.split(","))
    def remove_row_with_noise(x):
        for i in range(3, len(x)):
            if not x[i].isdecimal():
                return False
        return True
    pm25schema = weather_data_rdd.first()
    clean_weather_data = weather_data_rdd.filter(lambda x: x != pm25schema ).filter(remove_row_with_noise)
    """
        for x in clean_weather_data.take(30):
            for i in range(len(x)):
                print x[i],
            print ""
        """
    def Generated_Measurement(x):
        date=x[0]
        measure=x[2]
        measurements_of_a_day=[]
        for i in range(3,len(x)):
            measurements_of_a_day.append((date,measure,"hr"+str(i-3),x[i]))
        return measurements_of_a_day
    daliData =clean_weather_data.filter(lambda x: x[1] == u"大里" and (x[2] == u"PM2.5" or x[2] == u"PM10"))
    daliDataRow = \
        daliData \
            .flatMap(Generated_Measurement) \
            .map(lambda x: ((x[0], x[2]), x[1], x[3])) \
            .groupBy(lambda x: x[0]) \
            .filter(lambda x: len(x[1]) == 2) \
            .mapValues(lambda x: list(x)) \
            .mapValues(lambda x: [x[0][1], x[0][2], x[1][1], x[1][2]]) \
            .map(lambda x: [x[0][0], x[0][1], x[1][1], x[1][3]]) \
            .map(lambda x: Row(
            date=x[0],
            time=x[1],
            pm10=float(x[2]),
            pm25=float(x[3])
        ))
    df = sqlContext.createDataFrame(daliDataRow)
    corr_value=df.corr("pm10", "pm25")
    df.show(1500)
    print "corr=>",corr_value
