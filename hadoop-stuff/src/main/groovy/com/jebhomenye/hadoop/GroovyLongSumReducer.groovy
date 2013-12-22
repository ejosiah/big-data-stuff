package com.jebhomenye.hadoop

import java.io.IOException;

import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.Reducer.Context

class GroovyLongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	void reduce(Text word, Iterable<LongWritable> values, Context context){
		Long sum = values.collect{ it.get() }.sum()
		if(sum > 3){			
			context.write(word, new LongWritable(sum))
		}
	}

}
