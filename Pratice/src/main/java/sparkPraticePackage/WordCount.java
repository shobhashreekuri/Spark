package sparkPraticePackage;


import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {

	public static void main(String[] args) throws Exception{
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("in/word_count.text");
		JavaRDD<String> word = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		Map<String,Long> wordcounts = word.countByValue();
		
		for(Map.Entry<String, Long> entry : wordcounts.entrySet())
		{
			System.out.println(entry.getKey() + ":" +entry.getValue());
		}
		sc.close();
	}

}
