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

val station_logs = sc.textFile("/data/h31/station_logs/*")
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
station_log_reduced = station_log_reduced.sortByKey()
	
var prev_error = 0
var inst_station_log = station_log_reduced.filter(row => {
	val derivative = row._2 - prev_error
	prev_error = row._2

	if (derivative != 0) {
		true
	} else {
		false
	}
})

var inst_user_log = user_log_reduced.cartesian(inst_station_log).map(u => {
	if (u._1._1 == u._2._1) {
		(u._1._1, u._1._2)
	} else {
		(0, 0)
	}
})


inst_user_log = inst_user_log.filter(row => {
	if (row._1 != 0 && row._2 != 0) {
		true
	} else {
		false	
	}
})

val cov_mul = user_log_reduced.cartesian(station_log_reduced).map(u => {
	if (u._1._1 == u._2._1) {
		u._1._2 * u._2._2
	} else {
		1
	}
})

val avg_speed = user_log_reduced.join(station_log_reduced).map(row => (row._2._1).toString.toFloat).mean()
val avg_rspeed = inst_user_log.join(inst_station_log).map(row => (row._2._1).toString.toFloat).mean()
val avg_error = user_log_reduced.join(station_log_reduced).map(row => (row._2._2).toString.toFloat).mean()
val avg_rerror = inst_user_log.join(inst_station_log).map(row => (row._2._2).toString.toFloat).mean()

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


val weighted_speed = inst_user_log.join(inst_station_log).map(row => {
	val num_1 = (row._2._1).toString.toFloat
	val num_2 = (row._2._2).toString.toFloat
	num_1 * num_2
})

val weighted_avg = weighted_speed.sum() / weighted_speed.count()

val rspeed_sum_sq = inst_user_log.join(inst_station_log).map(row => {
	val num_1 = (row._2._1).toString.toFloat
	(num_1 - avg_rspeed)*(num_1 - avg_rspeed)
})

val CC = 0.95
val min_interval: Double = avg_rspeed - CC * scala.math.sqrt(rspeed_sum_sq.sum() / rspeed_sum_sq.count())
val max_interval: Double = avg_rspeed + CC * scala.math.sqrt(rspeed_sum_sq.sum() / rspeed_sum_sq.count())

result = result.union(sc.parallelize(Seq(("Cov", cov))))
result = result.union(sc.parallelize(Seq(("Weighted avg", weighted_avg))))
result = result.union(sc.parallelize(Seq(("Confidence min interval", min_interval))))
result = result.union(sc.parallelize(Seq(("Confidence max interval", max_interval))))

result.saveAsTextFile("/" + args(0) + "_result")
