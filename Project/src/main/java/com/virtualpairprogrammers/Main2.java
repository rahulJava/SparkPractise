package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main2 {

	public static void main(String[] args) 
	{
		List <Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(36);
		inputData.add(37);
		inputData.add(78);
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("starting spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Integer> myRDD =sc.parallelize(inputData);
		Math.sqrt(23);
		Integer result = myRDD.reduce((value1,value2)->value1 + value2);
		JavaRDD<Double> sqrtRDD=myRDD.map((value -> Math.sqrt(value)));
		sqrtRDD.foreach( value-> System.out.println(value));
		
		System.out.println(sqrtRDD.count());
		System.out.println(result);
		sc.close();
		
		
		
		
		// TODO Auto-generated method stub

	}

}
