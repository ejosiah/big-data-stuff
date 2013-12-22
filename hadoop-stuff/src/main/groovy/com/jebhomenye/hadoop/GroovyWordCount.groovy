package com.jebhomenye.hadoop

import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

class GroovyWordCount implements MapReduceJob {
	
	Class mapperClass() {
		TokenCountMapper.class;
	}

	Class reducerClass() {
		GroovyLongSumReducer.class;
	}

	Class combiner() {
		GroovyLongSumReducer.class;
	}

	Class jarByClass() {
		GroovyWordCount.class;
	}

	Class mapOutputKeyClass() {
		Text.class;
	}

	Class mapOutputValueClass() {
		LongWritable.class;
	}

}
