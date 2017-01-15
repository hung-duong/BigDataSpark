package com.myspark.jobs;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.myspark.helpers.Split;
import com.myspark.helpers.aggregate;

public class WordCount {
    private static JavaSparkContext sc;

	public static void main( String[] args ) throws Exception {
		if(args.length != 3) {
			System.err.println("Usage: WorkCount");
			System.exit(1);
		}
		
		String inputPath = args[0];
		String outputPath = args[1];
		int type =  Integer.parseInt(args[2]);

    	
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        sc = new JavaSparkContext(conf);
        JavaRDD<String> logdata = sc.textFile(inputPath);
        
        JavaRDD<String> words = logdata.flatMap(new Split(" "));
        
        //Count the namber of times each word appears
        JavaPairRDD<String, Integer> pairsWords = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		});
        
        JavaPairRDD<String, Integer> countsWord = pairsWords.reduceByKey(new aggregate()).sortByKey(true);
        
        if(type == 1) {
        	countsWord.saveAsTextFile(outputPath);
        	return;
        }
        
        
      //Filter out all words that show up less than 1 million times
        JavaPairRDD<String, Integer> countLessMillion = countsWord.filter(new Function<Tuple2<String,Integer>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> tuple) throws Exception {
				return tuple._2 < 1000000;
			}
		});
        
        if(type == 2) {
        	countLessMillion.saveAsTextFile(outputPath);
        }
        
        //For the remaining set, count the number of times each letter occurs
        JavaRDD<Character> letters = countLessMillion.flatMap(new FlatMapFunction<Tuple2<String,Integer>, Character>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Iterable<Character> call(Tuple2<String, Integer> t) throws Exception {
				List<Character> tupleList = new ArrayList<>();
				
				char[] cl = t._1.toCharArray();
				for(Character c : cl) {
					for(int i = 0; i < t._2; i++) {
						tupleList.add(c);
					}
				}
				
				return tupleList;
			}
		});
        
        JavaPairRDD<Character, Integer> pairLetters = letters.mapToPair(new PairFunction<Character, Character, Integer>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Tuple2<Character, Integer> call(Character t) throws Exception {
				return new Tuple2<Character, Integer>(t, 1);
			}
		});
       
        JavaPairRDD<Character, Integer> countLetters = pairLetters.reduceByKey(new aggregate()).sortByKey(true);
        
        if(type == 3) {
        	countLetters.saveAsTextFile(outputPath);
        }
	}
}
