package com.deloitte.twitter;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Displays the most positive hash tags by joining the streaming Twitter data
 * with a static RDD of the AFINN word list (http://neuro.imm.dtu.dk/wiki/AFINN)
 */
public class JavaTwitterHashTagJoinSentiments {

	public static void main(String[] args) throws IOException {
		/*
		 * if (args.length < 4) { System.err.
		 * println("Usage: JavaTwitterHashTagJoinSentiments <consumer key>" +
		 * " <consumer secret> <access token> <access token secret> [<filters>]"
		 * ); System.exit(1); }
		 */

		// StreamingExamples.setStreamingLogLevels();
		// Set logging level if log4j not configured (override by adding
		// log4j.properties to classpath)
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			Logger.getRootLogger().setLevel(Level.WARN);
		}

		final String consumerKey = "ejBosP2RDbrE9FmgOexgrIQmp";
		final String consumerSecret = "DD4zRQn0bOyNVRX5XF8Q8Q5Rgg3pbBVCiJMDgTsClycmsI2YGJ";
		final String accessToken = "142236084-x6f5QcRml9KRFT1DzV8tCGZo8b9U7wPh5vuKAqq3";
		final String accessTokenSecret = "6VMSQ53fID6MYXPeJfVf4N9h4S4uQs6yRvDZF2NUlFKbd";

		String[] filters = { "GST" };

		// Set the system properties so that Twitter4j library used by Twitter
		// stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

		SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments");

		// check Spark configuration for master URL, set it to local if not
		// configured
		if (!sparkConf.contains("spark.master")) {
			sparkConf.setMaster("local[2]");
		}

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);
		//final FileWriter fileWriter = new FileWriter(new File("Tweets.txt"));
		System.out.println("Stream data"+stream.toString());
		JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {

			@Override
			public Iterable<String> call(Status s) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(s.getText());
				//fileWriter.write(s.getText());
				return Arrays.asList(s.getText().split(" "));
			}
			/*
			 * @Override public Iterator<String> call(Status s) { return
			 * Arrays.asList(s.getText().split(" ")).iterator(); }
			 */
		});
		//fileWriter.close();
		/*
		 * System.out.println("hash value count "); words.foreachRDD(new
		 * Function<JavaRDD<String>, Void>() { public Void call(JavaRDD<String>
		 * rdd) throws Exception { FileWriter fileWriter = new FileWriter(new
		 * File("Tags.txt"), true); if (rdd != null) { List<String> result =
		 * rdd.collect();
		 * 
		 * for (String temp : result) { fileWriter.write(temp);
		 * fileWriter.write("\n"); ; System.out.println(temp); } }
		 * fileWriter.close(); return null; } });
		 * System.out.println("hash value count ends");
		 */
		JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String word) {
				return word.startsWith("#");
			}
		});

		/*
		 * words.foreachRDD(new Function<JavaRDD<String>, Void>() { public Void
		 * call(JavaRDD<String> rdd) throws Exception { FileWriter fileWriter =
		 * new FileWriter(new File("TagsFilter.txt"), true); if (rdd != null) {
		 * List<String> result = rdd.collect();
		 * 
		 * for (String temp : result) { fileWriter.write(temp);
		 * fileWriter.write("\n"); ; System.out.println(temp); } }
		 * fileWriter.close(); return null; } });
		 * System.out.println("hash value count " + hashTags.countByValue());
		 */
		// Read in the word-sentiment list and create a static RDD from it
		String wordSentimentFilePath = "C:\\Work\\Projects\\SparkStreamingExamples\\data\\AFINN-111.txt";
		final JavaPairRDD<String, Double> wordSentiments = jssc.sparkContext().textFile(wordSentimentFilePath)
				.mapToPair(new PairFunction<String, String, Double>() {
					@Override
					public Tuple2<String, Double> call(String line) {
						String[] columns = line.split("\t");
						return new Tuple2<String, Double>(columns[0], Double.parseDouble(columns[1]));
					}
				});

		JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				// leave out the # character
				return new Tuple2<String, Integer>(s.substring(1), 1);
			}
		});
		// print
		/*
		 * hashTagCount.foreachRDD(new Function2<JavaPairRDD<String, Integer>,
		 * Time, Void>() { FileWriter fileWriter = new FileWriter(new
		 * File("hashTagCount.txt"));
		 * 
		 * @Override public Void call(JavaPairRDD<String, Integer> e, Time arg1)
		 * throws Exception { // TODO Auto-generated method stub
		 * List<Tuple2<String, Integer>> rddList = e.collect(); for
		 * (Tuple2<String, Integer> rdd : rddList) { fileWriter.write(rdd._1 +
		 * " " + rdd._2); } fileWriter.close(); return null; }
		 * 
		 * });
		 */ JavaPairDStream<String, Integer> hashTagTotals = hashTagCount
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				}, new Duration(50000));

		// Determine the hash tags with the highest sentiment values by joining
		// the streaming RDD
		// with the static RDD inside the transform() method and then
		// multiplying
		// the frequency of the hash tag by its sentiment value
		JavaPairDStream<String, Tuple2<Double, Integer>> joinedTuples = hashTagTotals.transformToPair(
				new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Tuple2<Double, Integer>>>() {
					@Override
					public JavaPairRDD<String, Tuple2<Double, Integer>> call(JavaPairRDD<String, Integer> topicCount) {
						return wordSentiments.join(topicCount);
					}
				});

		JavaPairDStream<String, Double> topicHappiness = joinedTuples
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, Double>() {
					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Integer>> topicAndTuplePair) {
						Tuple2<Double, Integer> happinessAndCount = topicAndTuplePair._2();
						return new Tuple2<String, Double>(topicAndTuplePair._1(),
								happinessAndCount._1() * happinessAndCount._2());
					}
				});

		// Print hash tags with the most positive sentiment values
		topicHappiness.foreach(new Function<JavaPairRDD<String, Double>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, Double> happinessTopicPairs) {
				List<Tuple2<String, Double>> topList = happinessTopicPairs.take(100);
				System.out.println(String.format("\nHappiest topics  (%s total):", happinessTopicPairs.count()));
				for (Tuple2<String, Double> pair : topList) {
					System.out.println(String.format("%s (%s happiness)", pair._2(), pair._1()));
				}
				return null;
			}
		});

		JavaPairDStream<Double, String> happinessTopicPairs = topicHappiness
				.mapToPair(new PairFunction<Tuple2<String, Double>, Double, String>() {
					@Override
					public Tuple2<Double, String> call(Tuple2<String, Double> topicHappiness) {
						return new Tuple2<Double, String>(topicHappiness._2(), topicHappiness._1());
					}
				});

		JavaPairDStream<Double, String> happiest10 = happinessTopicPairs
				.transformToPair(new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
					@Override
					public JavaPairRDD<Double, String> call(JavaPairRDD<Double, String> happinessAndTopics) {
						return happinessAndTopics.sortByKey(false);
					}
				});

		// Print hash tags with the most positive sentiment values
		happinessTopicPairs.foreach(new Function<JavaPairRDD<Double, String>, Void>() {
			@Override
			public Void call(JavaPairRDD<Double, String> happinessTopicPairs) {
				List<Tuple2<Double, String>> topList = happinessTopicPairs.take(10);
				System.out.println(
						String.format("\nHappiest topics in last 10 seconds (%s total):", happinessTopicPairs.count()));
				for (Tuple2<Double, String> pair : topList) {
					System.out.println(String.format("%s (%s happiness)", pair._2(), pair._1()));
				}
				return null;
			}
		});

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}