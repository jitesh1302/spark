package com.deloitte.twitter;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

public class StaticTwitterSentimentAnalyzer {
	static Map<String, Integer> sentimentScoreMap = new TreeMap<String, Integer>();

	public static void main(String[] args) throws IOException {
		final String filter = args[0];
		System.out.println("filter is " + filter);
		SparkConf conf = new SparkConf().setAppName("Sentiment Analysis").setMaster("local[2]");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> tweetsFromFile = context.textFile("C:/Work/Projects/SparkStreamingExamples/data/Tweets.txt");
		// create a sentiment score RDD
		JavaRDD<String> wordScoreDictRDD = context
				.textFile("C:/Work/Projects/SparkStreamingExamples/data/AFINN-111.txt");
		JavaPairRDD<String, Integer> wordsSentimentRDD = wordScoreDictRDD
				.mapToPair(new PairFunction<String, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(String line) throws Exception {

						String[] split = line.split("=");
						return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
					}
				});

		JavaPairRDD<String, Integer> scoreTweetRDD = tweetsFromFile.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String line) throws Exception {
				// TODO Auto-generated method stub
				return line.contains(filter);
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String tweet) throws Exception {
				SentimentsScore score = new SentimentsScore(tweet);
				System.out.println("-----------------------------------------------------------");
				System.out.println(tweet+"--> "+score.calculate());
				System.out.println("-----------------------------------------------------------");
				return new Tuple2<String, Integer>("Sentiment score is ", score.calculate());
			}
		});
		JavaPairRDD<String, Integer> tweetSentimentResult = scoreTweetRDD
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer n1, Integer n2) throws Exception {
						// TODO Auto-generated method stub
						return n1 + n2;
					}
				});
		/*File file = new File("StaticSentimentresult");
		FileUtils.deleteDirectory(file);
		tweetSentimentResult.saveAsTextFile("StaticSentimentresult");*/
		List<Tuple2<String, Integer>> data = tweetSentimentResult.collect();
		for(Tuple2<String, Integer> line:data) {
			System.out.println();
			System.out.println("===================Sentiment Result====================");
			
			System.out.println(line);
			System.out.println("========================================================");
		}
	}
}
