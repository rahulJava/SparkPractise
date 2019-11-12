package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Example3 {

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
		JavaRDD<Integer> originalIntegers =sc.parallelize(inputData);
		
		//JavaRDD<IntegerWithSquareRoot > sqrtRDD=originalIntegers.map(value->new IntegerWithSquareRoot(value));
		
		JavaRDD <Tuple2<Integer, Double>> sqrtRDD = originalIntegers.map(value-> new Tuple2<Integer,Double>(value,Math.sqrt(value)));
		sqrtRDD.foreach( value-> System.out.println(value));
		sc.close();	
		
		
		
		
		// TODO Auto-generated method stub

	}

}
