package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkText {

	public static void main ( String [] args) {
		
		
		 Logger.getLogger("org.apache").setLevel( Level.WARN);
		  
		 System.setProperty("hadoop.home.dir", "c:/hadoop");
//		  SparkConf conf = new SparkConf().setAppName("MySpark")  remove ".setMaster("...."); when deploying to the emr 
		 SparkConf conf = new SparkConf().setAppName("MySpark").setMaster("local[*]");
		  JavaSparkContext sparkConn= new JavaSparkContext(conf);
		  
		  JavaRDD< String> readText= sparkConn.textFile("src/main/resources/subtitles/input.txt");
	// transformations
		  
	JavaRDD<String> replace =  readText.map(read -> read.replaceAll("[^a-zA-Z\\s]","").toLowerCase());
		
    JavaRDD<String> delSpace =  replace.filter(sentence -> sentence.trim().length()>0);
	
    JavaRDD<String> fmap= delSpace.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
    
    JavaRDD<String> removeEmptyline = fmap.filter(word -> word.trim().length()>0);
	
    JavaRDD<String> pickedup_word=  removeEmptyline.filter(word -> Util.isNotBoring(word));
  
    JavaPairRDD<String, Long> pairRdd = pickedup_word.mapToPair(word -> new Tuple2 <>(word,1L)); 
    
    JavaPairRDD<String, Long> reduceRdd =  pairRdd.reduceByKey((value1,value2) -> value1+value2);
    
//    JavaPairRDD<String, Long>  sort = reduceRdd.sortByKey();
    JavaPairRDD<Long, String> switchKV= reduceRdd.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
	
    JavaPairRDD<Long, String> sorted =  switchKV.sortByKey(false);
   // sorted = sorted.coalesce(1);
    
  // actions.  
   List<Tuple2<Long, String>> list = sorted.collect();
//		
  list.forEach(System.out::println);
    
//		
//   System.out.println("no of partitions :"+ sorted.getNumPartitions());
// sorted.foreach(data -> System.out.println(data));	
//		
		
//		  .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
//		 //  .filter(word -> word.length() > 1)
//		   .foreach(value -> System.out.println(value));
//          
	
		  sparkConn.close();
	}
	
}
