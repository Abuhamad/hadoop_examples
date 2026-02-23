WeatherAnalysis MapReduce Jobs
==============================

This folder contains three Hadoop MapReduce jobs that analyze daily temperatures
from a simple CSV input. Run Mean.java and WeatherStats.java first, then use
SortWeather.java to sort the WeatherStats output by mean temperature.

Input format
------------
Each line is a CSV row with three fields:

		ddMMyyyy,minTemp,maxTemp

Example data is in Assets/WeatherAnalysis/milano_temps.csv.

Jobs (simple description)
-------------------------
Mean.java
- Mapper parses each line, groups max temperatures by month (MMyyyy), and emits
	a SumCount (sum and count) in cleanup to reduce network traffic.
- Reducer merges SumCount values and outputs the mean max temperature per month.

WeatherStats.java
- Mapper performs in-mapper combining and builds partial stats per month for one
	target column (column 2 by default: max temperature).
- Reducer merges stats and outputs: min, max, mean, and standard deviation per
	month.

SortWeather.java
- Mapper reads WeatherStats output, extracts the Mean value, and emits it as the
	key so Hadoop can sort by temperature.
- Custom comparator sorts mean temperature in descending order.
- Reducer outputs the original lines in sorted order (single reducer for global
	ordering).

Build a runnable jar
--------------------
From the WeatherAnalysis directory:

		mkdir -p build/classes
		
        javac -classpath "$(hadoop classpath)" -d build/classes \
				Mean.java WeatherStats.java SortWeather.java
		
        jar -cvf weather-analysis.jar -C build/classes .

Run the jobs
------------
1) Put input on HDFS (example path):

		hadoop fs -mkdir -p /user/$USER/weather/input
		hadoop fs -copyFromLocal Assets/WeatherAnalysis/milano_temps.csv /user/$USER/weather/input/

2) Run Mean and WeatherStats:

		hadoop jar weather-analysis.jar Mean \
				/user/$USER/weather/input /user/$USER/weather/mean_out

		hadoop jar weather-analysis.jar WeatherStats \
				/user/$USER/weather/input /user/$USER/weather/stats_out

3) Sort WeatherStats output by mean (descending):

		hadoop jar weather-analysis.jar SortWeather \
				/user/$USER/weather/stats_out /user/$USER/weather/sorted_out

4) View results:

		hadoop fs -cat /user/$USER/weather/mean_out/part-r-00000 | head
		
        hadoop fs -cat /user/$USER/weather/stats_out/part-r-00000 | head
		
        hadoop fs -cat /user/$USER/weather/sorted_out/part-r-00000 | head

**Note:** If an output directory already exists, delete it before rerunning:

		hadoop fs -rm -r /user/$USER/weather/mean_out
		
        hadoop fs -rm -r /user/$USER/weather/stats_out
		
        hadoop fs -rm -r /user/$USER/weather/sorted_out
