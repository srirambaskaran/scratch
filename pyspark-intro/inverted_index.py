from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Inverted index")
conf = conf.setMaster("local[*]")
sc   = SparkContext(conf=conf)

file_contents_rdd = sc.textFile("big.txt")

non_empty_lines = file_contents_rdd.filter(lambda line: line != None and line != "")

word_counts = non_empty_lines.flatMap(lambda line: line.split(" ")) \
				.map(lambda token: (token, 1)) \
				.reduceByKey(lambda x,y: x+y)


counts = word_counts.collect()

print "\n".join(map(lambda x: str(x), counts))