#-*-coding:utf-8 -*-
#2015PM2.5大里top5
import random
import os
os.chdir("C:\Python27")
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree

from pyspark.mllib.evaluation import BinaryClassificationMetrics

if __name__ == "__main__":
    sc = SparkContext("local", "PythonWordCount")
    sqlContext = SQLContext(sc)
    weather_data = sc.textFile("C:/Users/user/Desktop/pm2.5Taiwan.csv", 1)


    def remove_row_with_noise(x):
        for i in range(3, len(x)):
            if not x[i].isdecimal():
                return False
        return True


    weather_data_rdd = weather_data.map(lambda line: line.split(","))
    pm25schema = weather_data_rdd.first()
    daliData = weather_data_rdd.filter(lambda x: x != pm25schema and x[1] == u"大里" and (x[2] == u"PM2.5" or x[2] == u"PM10" or x[2] == u"AMB_TEMP") ).filter(remove_row_with_noise)
    #a=daliData.take(800)
    dalipm25row = daliData.map(lambda p: Row(
        date=p[0],
        location=p[1],
        measure=p[2],
        hr_01=float(p[3]), hr_02=float(p[4]), hr_03=float(p[5]), hr_04=float(p[6]), hr_05=float(p[7]),
        hr_06=float(p[8]), hr_07=float(p[9]), hr_08=float(p[10]), hr_09=float(p[11]), hr_10=float(p[12]),
        hr_11=float(p[13]), hr_12=float(p[14]), hr_13=float(p[15]), hr_14=float(p[16]), hr_15=float(p[17]),
        hr_16=float(p[18]), hr_17=float(p[19]), hr_18=float(p[20]), hr_19=float(p[21]), hr_20=float(p[22]),
        hr_21=float(p[23]), hr_22=float(p[24]), hr_23=float(p[25]), hr_24=float(p[26]),
    )
                               )
    df = sqlContext.createDataFrame(dalipm25row)
    #df.show()
    df_pm10 = df.select(df.date.alias("datepm10"), df.hr_09.alias("hr_09_pm10"), df.hr_10.alias("hr_10_pm10"),
                        df.hr_11.alias("hr_11_pm10"), df.hr_12.alias("hr_12_pm10"), "measure").filter(
        df.measure == "PM10")
    df_pm25 = df.select(df.date.alias("datepm25"), df.hr_09.alias("hr_09_pm25"), df.hr_10.alias("hr_10_pm25"),
                        df.hr_11.alias("hr_11_pm25"), df.hr_12.alias("hr_12_pm25"), "measure").filter(
        df.measure == "PM2.5")
    df_AMB_TEMP = df.select(df.date.alias("dateAMB_TEMP"), df.hr_09.alias("hr_09_AMB_TEMP"),
                            df.hr_10.alias("hr_10_AMB_TEMP"), df.hr_11.alias("hr_11_AMB_TEMP"),
                            df.hr_12.alias("hr_12_AMB_TEMP"), "measure").filter(df.measure == "AMB_TEMP")
    traing_data = df_pm25 \
        .join(df_pm10, df_pm25.datepm25 == df_pm10.datepm10).drop("measure")\
        .join(df_AMB_TEMP, df_pm25.datepm25 == df_AMB_TEMP.dateAMB_TEMP).drop("dateAMB_TEMP").drop("measure").drop(
        "datepm10")
    #traing_data.show()
    traing_data.corr("hr_09_pm25", "hr_10_pm25")

    #traing_data.show()

    formulated_traning_data = traing_data.select("*",
                                                 F.when(traing_data.hr_12_pm25 > 50, 1).otherwise(0)).withColumnRenamed(
        "CASE WHEN (hr_12_pm25 > 50) THEN 1 ELSE 0", "pm25_condiction"). \
        drop("hr_12_AMB_TEMP").drop("hr_12_pm10").drop("hr_12_pm25").drop("datepm25")
    formulated_traning_data.show()


    LabelPoints = formulated_traning_data.rdd \
        .map(lambda r: LabeledPoint(r.pm25_condiction,
                                    [r.hr_09_pm25, r.hr_10_pm25, r.hr_11_pm25,
                                     r.hr_09_pm10, r.hr_10_pm10, r.hr_11_pm10,
                                     r.hr_09_AMB_TEMP, r.hr_10_AMB_TEMP, r.hr_11_AMB_TEMP]))
    (trainData, validationData, testData) = LabelPoints.randomSplit([6, 0, 4])

    DTModel = DecisionTree.trainClassifier(trainData,
                                           numClasses=2,
                                           categoricalFeaturesInfo={},
                                           impurity="entropy",
                                           maxDepth=3,
                                           maxBins=3)
    prediction = DTModel.predict(testData.map(lambda x: x.features))
    predictionAndLabels = prediction.zip(testData.map(lambda x: x.label))
    predictionAndLabels.collect()


    metrics = BinaryClassificationMetrics(predictionAndLabels)
    print metrics.areaUnderROC