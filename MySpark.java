package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;



public class MySpark {


  public static void main(String[] args) {
//		List<Integer> input = new ArrayList<>();
//		input.add(12);
//		input.add(12);
//		input.add(12);
//		input.add(12);
//		input.add(12);
//		
	  
	  List<String> input = new ArrayList<>();
		input.add("Warning: yore newbie 0405");
		input.add("Warning:your new to course 1632");
		input.add("error: you are at new place  1854");
		
		
	/*input.forEach(System.out::println);*/
  Logger.getLogger("org.apache").setLevel( Level.WARN);
  
  SparkConf conf = new SparkConf().setAppName("MySpark").setMaster("local[*]");
  
  JavaSparkContext sparkConn= new JavaSparkContext(conf);
  
 JavaRDD<String> statement = sparkConn.parallelize(input);
 // flat map  
  JavaRDD<String> words = statement.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//  JavaRDD<String> filterword = words.filter(word -> word.length() > 1);

  
  // single liner for filter 
sparkConn.parallelize(input)

.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
 .filter(word -> word.length() > 1)
 .foreach(value -> System.out.println(value));

 // filterword.foreach(value->System.out.println(value));
  //words.foreach(System.out::println);
  
words.foreach(value -> System.out.println(value));
  
//  JavaRDD<String> myRdd= sparkConn.parallelize(input)
//  single line method reduce by key  
  sparkConn.parallelize(input)
          .mapToPair(rowValue ->new Tuple2<>(rowValue.split(":")[0],1L))
		  .reduceByKey((value1,value2)-> value1+value2)
		  .foreach(tuple -> System.out.println(tuple._1+ " has "+ tuple._2+"instances"));
 
//JavaPairRDD<String , Long> pair = myRdd.mapToPair(rowValue -> { 
//	  
//	 String [] col = rowValue.split(":");
//	 String level = col[0];
//		 return new Tuple2<>(level,1L);
//  });
  
//  group by key 
  
//  sparkConn.parallelize(input)
//		.mapToPair(rowValue ->new Tuple2<>(rowValue.split(":")[0],1L))
//		.groupByKey()
//		.foreach(tuple -> System.out.println(tuple._1+ " has "+ Iterables.size(tuple._2)+"instances"));
  
  
// JavaPairRDD<String,Long>Sum =pair.reduceByKey((value1,value2)-> value1+value2);
// Sum.foreach(tuple -> System.out.println(tuple._1+ " has "+ tuple._2+"instances"));
  
//  Integer result = myRdd.reduce((value1,value2)->value1+value2);
//  
 //JavaRDD<SquareRoot> data = myRdd.map(Value ->  new SquareRoot(Value));
 
//// JavaRDD<Tuple2<Integer,Double>> tData = myRdd.map(Value ->  new Tuple2<>(Value, Math.sqrt(Value)));
////  Tuple2<Integer,Double> mayValue = new Tuple2<>(9,3.0);
////  
//  System.out.println(tData);
 //  
//  data.foreach(value->System.out.println(value));
//  
  // map reduce 
  
//  JavaRDD<Integer> dataCount =data.map(value-> 1);
//  Integer count = dataCount.reduce((value1,value2)->value1+value2);
//  
//  
//  
//  System.out.println(count);
//  
//  System.out.println(data);
// 
//  System.out.println(result);
//  
//  System.out.println(data.count());
  
 
  
  sparkConn.close();

}
}
