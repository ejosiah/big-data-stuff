package com.jebhomenye.hadoop;


@SuppressWarnings("all")
public interface MapReduceJob {
	
	Class mapperClass();
	
	Class reducerClass();
	
	Class combiner();
	
	Class jarByClass();
	
	Class mapOutputKeyClass();
	
	Class mapOutputValueClass();
	
}
