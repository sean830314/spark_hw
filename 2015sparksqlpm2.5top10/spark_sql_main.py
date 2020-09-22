#-*-coding:utf-8 -*-
#Spark Sql  2015 PM2.5 top10
import random
import os
os.chdir("C:\Python27")
from pyspark import SparkContext
from pyspark.sql import SQLContext
if __name__ == "__main__":

    sc = SparkContext("local", "PythonWordCount")
    sqlContext = SQLContext(sc)
    weather_data = sc.textFile("C:/Users/ekko/PycharmProjects/2015sparksqlpm2.5top10/pm2.5Taiwan.csv", 1)
    weather_data_rdd = weather_data.map(lambda line: line.split(","))
    def remove_row_with_noise(x):
        for i in range(3, len(x)):
            if not x[i].isdecimal():
                return False
        return True
    pm25schema = weather_data_rdd.first()
    clean_weather_data = weather_data_rdd.filter(lambda x: x != pm25schema and x[2] == "PM2.5").filter(remove_row_with_noise)
    from pyspark.sql import Row
    dalipm25row = clean_weather_data.map(lambda p:
                               Row(
                                   date=p[0],
                                   location=p[1],
                                   measure=p[2],
                                   hr_01=float(p[3]), hr_02=float(p[4]), hr_03=float(p[5]), hr_04=float(p[6]),
                                   hr_05=float(p[7]),
                                   hr_06=float(p[8]), hr_07=float(p[9]), hr_08=float(p[10]), hr_09=float(p[11]),
                                   hr_10=float(p[12]),
                                   hr_11=float(p[13]), hr_12=float(p[14]), hr_13=float(p[15]), hr_14=float(p[16]),
                                   hr_15=float(p[17]),
                                   hr_16=float(p[18]), hr_17=float(p[19]), hr_18=float(p[20]), hr_19=float(p[21]),
                                   hr_20=float(p[22]),
                                   hr_21=float(p[23]), hr_22=float(p[24]), hr_23=float(p[25]), hr_24=float(p[26]),
                               )
                            )

    df = sqlContext.createDataFrame(dalipm25row)
    df.registerTempTable("DaliTable")
    sqlContext.sql("""
                select  date,location, hr_01+hr_02+hr_03+hr_04+hr_05+hr_06+hr_07+hr_08+hr_09+hr_10+hr_11+hr_12+hr_13+hr_14+hr_15+hr_16+hr_17+hr_18+hr_19+hr_20+hr_21+hr_22+hr_23+hr_24 as sum 
                from DaliTable 
                order by sum DESC Limit 10
                """).show()
"""    
    def Max_data(x):
        max = 0
        stri = "#"
        for i in range(3, len(x)):
            if x[i] > max:
                stri = "#" + str(i - 3)
                max = x[i]
        return x[0] + x[1] + stri + "," + max
    a = clean_weather_data.map(Max_data).map(lambda word: (word.split(",")[0], int(word.split(",")[1], 10))).sortBy(lambda x: x[1], ascending=False)
    for x in a.take(5):
        for i in range(len(x)):
            print x[i],
        print
"""