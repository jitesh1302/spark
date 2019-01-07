package com.deloitte.twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class SentimentsScore implements Serializable {
	int tweetScore = 0;
	String tweet;
	Map<String, Integer> sentimentScoreMap = new TreeMap<String, Integer>();

	public SentimentsScore(String tweet) throws IOException {
		this.tweet = tweet;
		FileReader fileReader = new FileReader(
				new File("C:\\Work\\Projects\\SparkStreamingExamples\\data\\AFINN-111.txt"));
		BufferedReader br = new BufferedReader(fileReader);
		String line;
		while ((line = br.readLine()) != null) {
			String[] spilt = line.split("=");
			sentimentScoreMap.put(spilt[0], Integer.parseInt(spilt[1]));
		}
	}

	public int calculate() {
		String[] split = tweet.split(" ");
		tweetScore=0;
		for (String word : split) {
			String lowerCaseWord = "";
			try {
				lowerCaseWord = word.toLowerCase();
			} catch (Exception e) {
				lowerCaseWord = "";
			}
			if (sentimentScoreMap.containsKey(lowerCaseWord)) {
				tweetScore = tweetScore + sentimentScoreMap.get(lowerCaseWord);
			}
		}
		return tweetScore;

	}
}
