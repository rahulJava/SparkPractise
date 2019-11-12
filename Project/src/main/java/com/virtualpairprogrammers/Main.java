package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;;

public class Main {

	public static void main(String[] args) 
	{	
		List <Double> inputData = new ArrayList<>();
		inputData.add(35.5);
		inputData.add(36.6);
		inputData.add(37.7);
		inputData.add(78.8);
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("starting spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Double> myRDD =sc.parallelize(inputData);
		
		Double result = myRDD.reduce((value1,value2)->value1 + value2);
		Tuple2<Integer,Double> myValue = new Tuple2<>(9,3.0);
		
		System.out.println(result);
		sc.close();
		
		
		
		
		// TODO Auto-generated method stub

	}

}
