package com.deloitte.twitter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Function1;
import scala.Tuple1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.NullAuthorization;

/*
 * this class gives the top 10 trending tweets .
 */
public class TrendingTweets {

	public static void main(String[] args) throws IOException {

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Popular Hashtags");
		//String[] filters = { "GST" };
		// Authorization authorization = NullAuthorization.getInstance();

		// set the oath parameters
		System.setProperty("twitter4j.oauth.consumerKey", "ejBosP2RDbrE9FmgOexgrIQmp");
		System.setProperty("twitter4j.oauth.consumerSecret", "DD4zRQn0bOyNVRX5XF8Q8Q5Rgg3pbBVCiJMDgTsClycmsI2YGJ");
		System.setProperty("twitter4j.oauth.accessToken", "142236084-x6f5QcRml9KRFT1DzV8tCGZo8b9U7wPh5vuKAqq3");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "6VMSQ53fID6MYXPeJfVf4N9h4S4uQs6yRvDZF2NUlFKbd");

		// spark streaming context
		JavaStreamingContext context = new JavaStreamingContext(conf, new Duration(20000));

		// create twitter stream
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(context);

		// Split the stream on space and extract hashtags
		JavaDStream<String> hashtags = stream.flatMap(new FlatMapFunction<Status, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Status e) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(e.getText().split(" "));
			}
		}).filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String e) throws Exception {
				// TODO Auto-generated method stub
				return e.startsWith("#");
			}
		});

		// Get the top hashtags over the previous 60 sec window
		JavaPairDStream<String, Integer> mapHashtags = hashtags.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String e) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<String, Integer>(e, 1);
			}
		}

		);

		JavaPairDStream<Integer, String> topCount60 = mapHashtags
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer a, Integer b) throws Exception {
						// TODO Auto-generated method stub
						return a + b;
					}
				}, Seconds.apply(60l)).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> record) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Integer, String>(record._2, record._1);
					}

				}).transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> e) throws Exception {
						// TODO Auto-generated method stub
						return e.sortByKey(false);
					}

				});

		// Print top 10 popular hashtags
		
		topCount60.foreach(new Function<JavaPairRDD<Integer, String>, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<Integer, String> e) throws Exception {
				System.out.println("--------------------HashTags----------------------------");
				List<Tuple2<Integer, String>> topList = e.take(10);
				for (Tuple2<Integer, String> tweets : topList) {
					
					System.out.println(tweets._2 + "," + tweets._1);
				}
				System.out.println("-------------------------------------------");
				return null;

			}
		});
		
		context.start();
		context.awaitTermination();
	}

}
