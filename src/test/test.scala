sc
  .textFile("/lenta_articles/*.txt", 4)
  .flatMap(line => line.split(" ")) 
  .map(word => (word, 1))
  .reduceByKey(_+_)
  .sortByKey()
  .saveAsTextFile("/lenta_sorted_result")

