package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class Joins {

	public static void main(String[] args) {
		 Logger.getLogger("org.apache").setLevel( Level.WARN);
		  
		 System.setProperty("hadoop.home.dir", "c:/hadoop"); 
		 SparkConf conf = new SparkConf().setAppName("MySpark").setMaster("local[*]");
		 JavaSparkContext sparkConn= new JavaSparkContext(conf);
	  
		 List<Tuple2<Integer,Integer>> visits =new ArrayList<>();
		 visits.add(new Tuple2<>(1,16));
		 visits.add(new Tuple2<>(2,20));
		 visits.add(new Tuple2<>(3,30));
		 visits.add(new Tuple2<>(4,40));
		 
		 
		 List<Tuple2<Integer,String>> user =new ArrayList<>();
		 user.add(new Tuple2<>(1,"sai"));
		 user.add(new Tuple2<>(2,"ss"));
		 user.add(new Tuple2<>(3,"ssk"));
		 user.add(new Tuple2<>(4,"kumar"));
		 
 JavaPairRDD< Integer, Integer> uservisited = sparkConn.parallelizePairs(visits);
 JavaPairRDD< Integer, String> userName = sparkConn.parallelizePairs(user);

// join ->JavaPairRDD<Integer, Tuple2<Integer, String>> joinRDD = uservisited.join(userName);		 
// left outer join -> JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinRDD = uservisited.leftOuterJoin(userName);		 	
//joinRDD.foreach(value -> System.out.println(value._2._2.get().toUpperCase()));
// Right outter join -->JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinRDD = uservisited.rightOuterJoin(userName);
// joinRDD.foreach(value -> System.out.println(value._2._2+" "+"had"+" "+ value._2._1.orElse(0)+"  "+ "visits"));
 // full join ( cartesion)   
 JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinRDD = uservisited.cartesian(userName);
 
 joinRDD.foreach(value->System.out.println(value));
 sparkConn.close();
	}
	
	

}
