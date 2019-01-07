/*package com.deloitte.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

*//**
 * Spark STreaming example
 *
 *//*
public class SocketSparkStreaming {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Socket Streaming");
		JavaStreamingContext context = new JavaStreamingContext(conf, new Duration(1200));
		// creating receiver for input stream

		JavaReceiverInputDStream<String> lines = context.socketTextStream("localhost", 9999);

		// Split each line into words
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			public Iterator<String> call(String val) throws Exception {
				return Arrays.asList(val.split(" ")).iterator();

			}

		});

		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			public Tuple2<String, Integer> call(String s) throws Exception {

				return new Tuple2(s, 1);
			}
		});
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			public Integer call(Integer count1, Integer count2) throws Exception {

				return count1 + count2;
			}
		});

		wordCounts.print();
		context.start();
		context.awaitTermination();
	}
}
*/