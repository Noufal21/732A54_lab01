

from pyspark import SparkContext
import shutil
import os
if os.path.isdir('BDA'):
    shutil.rmtree('BDA')

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
ostergotland_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
lines = temperature_file.map(lambda line: line.split(";"))
precipitation_lines  = precipitation_file.map(lambda line: line.split(";"))
ostergotland_lines = ostergotland_file.map(lambda line : line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], (x[0],float(x[3]))))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Get max
max_temperatures = year_temperature.reduceByKey(lambda a,b: (a[0], max(a[1], b[1]) , min(a[1], b[1]) ))
max_temperatures = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temperatures.saveAsTextFile("BDA/question1")


year_month_temperature = lines.map(lambda x: ((x[1][0:4], x[1][5:7]),(x[0], float(x[3]) ,1)))
year_month_temperature = year_month_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014)

greater_10_temperature = year_month_temperature.filter(lambda x: float(x[1][1]) >=10)
greater_10_temperature = greater_10_temperature.map(lambda x: (x[0],1))
greater_10_temperature_count = greater_10_temperature.reduceByKey(lambda x,y:x+y)
greater_10_temperature_count.saveAsTextFile("BDA/question2_1")


greater_10_temperature = year_month_temperature.filter(lambda x: float(x[1][1]) >=10)
greater_10_temperature_mapped = greater_10_temperature.groupByKey().mapValues(list).map(lambda x: (x[0],{item[0] for item in x[1]},1 ))
greater_10_temperature_mapped_count = greater_10_temperature_mapped.map(lambda x: (x[0],len(x[1])))
greater_10_temperature_mapped_count.saveAsTextFile("BDA/question2_2")



year_month_sation_temperature = lines.map(lambda x: ((x[1][0:4], x[1][5:7],x[0]),( float(x[3]),1)))
year_month_sation_temperature = year_month_sation_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014)
sum_year_month_sation_temperature = year_month_sation_temperature.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
avg_year_month_sation_temperature = sum_year_month_sation_temperature.map(lambda x: (x[0],x[1][0]/x[1][1]))
avg_year_month_sation_temperature.saveAsTextFile("BDA/question3")


station_maximum_temperature = lines.map(lambda x: ((x[0]),( float(x[3]))))
station_maximum_temperature = station_maximum_temperature.reduceByKey(lambda x,y: max(x,y))
station_maximum_temperature = station_maximum_temperature.filter(lambda x:float(x[1])>= 25 and float(x[1]) <=30)

station_maximum_precipitation = precipitation_lines.map(lambda x: ((x[1][0:4], x[1][5:7],x[1][8:10],x[0]),float(x[3])))
station_maximum_precipitation = station_maximum_precipitation.reduceByKey(lambda x,y: x+y)
station_maximum_precipitation_mapped = station_maximum_precipitation.map(lambda x: (x[0][3],x[1]))
station_maximum_precipitation_mapped = station_maximum_precipitation_mapped.reduceByKey(lambda x,y: max(x,y))
station_maximum_precipitation_mapped = station_maximum_precipitation_mapped.filter(lambda x: float(x[1]) >10 and float(x[1]) <=200)
join_keys = station_maximum_temperature.join(station_maximum_precipitation_mapped).map(lambda x: ((x[0],x[1][0]),[x[1][1]]))
join_keys = join_keys.reduceByKey(lambda x,y: x+y)
join_keys.saveAsTextFile("BDA/question4")


station_precipitation = precipitation_lines.map(lambda x: (     (x[1][0:4], x[1][5:7],x[0]),   float(x[3])   ))
station_precipitation = station_precipitation.filter(lambda x: int(x[0][0])>=1993 and int(x[0][0])<=2016)
station_sum_precipitation = station_precipitation.reduceByKey(lambda x,y:x+y)

#station_average_precipitation = station_sum_precipitation.map(lambda x: (x[0],x[1][0]/x[1][1]))


ostergotland_stations = list(ostergotland_lines.map(lambda line: line[0]).collect())
ostergotland_stations_average_precipitation = station_sum_precipitation.filter(lambda x: x[0][2] in ostergotland_stations )

ostergotland_stations_average_precipitation = ostergotland_stations_average_precipitation.map(lambda x: ((x[0][0],x[0][1]), (x[1],1)))
ostergotland_stations_average_precipitation = ostergotland_stations_average_precipitation.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
ostergotland_stations_average_precipitation = ostergotland_stations_average_precipitation.map(lambda x: (x[0],x[1][0]/x[1][1]))
ostergotland_stations_average_precipitation.saveAsTextFile("BDA/question5")
