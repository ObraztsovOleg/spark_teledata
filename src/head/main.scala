import org.apache.spark.mllib.stat.Statistics
var result = spark.sparkContext.emptyRDD[(String, Double)]
val args = spark.sqlContext.getConf("spark.driver.args").split(",")

val user_logs = sc.textFile("/data/" + args(0) + "/user_logs/*")
val user_log_splited = user_logs.flatMap(line => List(line.split(",\t")))
val user_log_filtered = 
	user_log_splited.filter(row => {
		if (util.Try(row(0).toInt).isSuccess ||
		    util.Try(row(1).toInt).isSuccess ||
		    util.Try(row(2).toFloat).isSuccess) {
			true
		} else {
			false
		}
	})
val user_log_speed =
	user_log_filtered.map(row => {
		val time = (row(0).toInt * 86400 + row(1).toInt) / 300
		(time,row(2).toFloat)
	})
val user_log_reduced = user_log_speed.reduceByKey(_+_)

val station_logs = sc.textFile("/data/" + args(0) + "/station_logs/*")
val station_logs_splited = station_logs.flatMap(line => List(line.split(",\t")))
val station_logs_filtered =
	station_logs_splited.filter(row => {
		if (util.Try(row(0).toInt).isSuccess ||
		    util.Try(row(1).toInt).isSuccess ||
		    util.Try(row(2).toInt).isSuccess) {
			true
		} else {
			false
		}
	})
val station_log_error = 
	station_logs_filtered.map(row => {
		val time = (row(0).toInt * 86400 + row(1).toInt) / 300
		(time,row(2).toInt)
	})
var station_log_reduced = station_log_error.reduceByKey(_ + _)
	
var prev_error: Float = 0
var df = station_log_reduced.toDF()
df = df.sort("_1")
df.show()

var mapped_df = df.map(row => {
	val num_val = row.get(1).toString().toFloat
	val false_val: Float = 0
	
	val derivative = num_val - prev_error
	prev_error = num_val
	

	if (derivative > 0) {
		(row.get(0).toString().toInt, row.get(1).toString().toFloat)
	} else {
		(row.get(0).toString().toInt, false_val)
	}
})



var filtered_df = mapped_df.filter("_2 != 0")
val inst_station_log = filtered_df.rdd

val rddX = user_log_reduced.join(station_log_reduced).map(row => (row._2._1).toString.toDouble)
val rddY = user_log_reduced.join(station_log_reduced).map(row => (row._2._2).toString.toDouble)
val correlation: Double = Statistics.corr(rddX, rddY, "pearson")

var inst_user_log = user_log_reduced.join(inst_station_log)

val avg_speed = user_log_reduced.join(station_log_reduced).map(row => (row._2._1).toString.toFloat).mean()
val avg_rspeed = inst_user_log.join(inst_station_log).sortByKey().map(row => (row._2._1._1).toString.toFloat).mean()
val avg_error = user_log_reduced.join(station_log_reduced).map(row => (row._2._2).toString.toFloat).mean()
val avg_rerror = inst_user_log.join(inst_station_log).sortByKey().map(row => (row._2._2).toString.toFloat).mean()

inst_user_log.join(inst_station_log).foreach(println)

val nominator = user_log_reduced.join(station_log_reduced).map(row => {
	val num_1 = (row._2._1).toString.toFloat
	val num_2 = (row._2._2).toString.toFloat
	(num_1 - avg_speed)*(num_2 - avg_error)
}).sum()

val x_denominator = user_log_reduced.join(station_log_reduced).map(row => {
	val num_1 = (row._2._1).toString.toFloat
	(num_1 - avg_speed) * (num_1 - avg_speed)
}).sum()

val y_denominator = user_log_reduced.join(station_log_reduced).map(row => {
	val num_2 = (row._2._2).toString.toFloat
	(num_2 - avg_error) * (num_2 - avg_error)
}).sum()

val cov: Double = nominator / scala.math.sqrt(x_denominator * y_denominator)


val weighted_speed = inst_user_log.join(inst_station_log).map(row => row._2._1._1 * row._2._1._2)
val weighted_error = inst_user_log.join(inst_station_log).map(row => row._2._1._2)
val weighted_avg = weighted_speed.sum() / weighted_error.sum()

val rspeed_sum_sq = inst_user_log.join(inst_station_log).map(row => {	
	val num_1 = (row._2._1._1).toString.toFloat
	(num_1 - avg_rspeed)*(num_1 - avg_rspeed)
})

avg_rspeed

val CC = 0.95
val min_interval: Double = avg_rspeed - CC * scala.math.sqrt(rspeed_sum_sq.sum() / rspeed_sum_sq.count())
val max_interval: Double = avg_rspeed + CC * scala.math.sqrt(rspeed_sum_sq.sum() / rspeed_sum_sq.count())

result = result.union(sc.parallelize(Seq(("Cov", cov))))
result = result.union(sc.parallelize(Seq(("Weighted avg", weighted_avg))))
result = result.union(sc.parallelize(Seq(("Confidence min interval", min_interval))))
result = result.union(sc.parallelize(Seq(("Confidence max interval", max_interval))))

result.saveAsTextFile("/" + args(0) + "_result")
