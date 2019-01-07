package com.deloitte.twitter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class TwitterSentimentAnalyzer implements Serializable {

	static Map<String, Integer> sentimentScoreMap = new TreeMap<String, Integer>();

	public static void main(String[] args) throws IOException {
		// System.out.println("Hello");
		final String consumerKey = "a7srPX4RZZdMsSpFxEv56MHg9";
		final String consumerSecret = "wJbV1XdNumLAspTgcX4Ah6S5I8eYSTkAfEHT6siL8xJY36dSyP";
		final String accessToken = "142236084-XLYe9SbhjMDIKx0tpJ3hvMMZqwHNxbmAVPXpMONJ";
		final String accessTokenSecret = "5GnHRtFZJqFKBMBtNwGLcSkUd0JXYUFwRNceWlc8BiXwW";
		final String filter;
		if (args.length == 0) {
			filter = "";
		} else {
			filter = args[0];
		}
		System.out.println("filter = " + filter);
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterHelloWorldExample");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(40000));

		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

		// filter only english tweets
		JavaDStream<Status> englishTweets = twitterStream.filter(new Function<Status, Boolean>() {
			@Override
			public Boolean call(Status status) throws Exception {
				if (filter == "") {
					return true;
				}
				return status.getText().contains(filter);
			}
		});

		// checking tweets and dictionary

		JavaPairDStream<String, Integer> scoredDStream = englishTweets
				.mapToPair(new PairFunction<Status, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(Status s) throws Exception {

						// System.out.println(s.getText());
						SentimentsScore score = new SentimentsScore(s.getText());
						System.out.println("---------------------------------------------");
						int count = score.calculate();
						System.out.println(s.getText() + "---->" + count);
						System.out.println("---------------------------------------------");
						return new Tuple2<String, Integer>("Sentiment", count);
					}
				});

		// scoredDStream.print();

		JavaDStream<Tuple2<String, Integer>> sentimentRDD = scoredDStream
				.reduce(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2)
							throws Exception {

						return new Tuple2<String, Integer>(t1._1, t1._2 + t2._2);
					}
				});
		System.out.println("===========================");
		sentimentRDD.print();
		System.out.println("===========================");
		jssc.start();
		jssc.awaitTermination();
	}
}
