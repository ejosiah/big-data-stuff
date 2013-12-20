package com.jebhomenye.hadoop

import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

class GroovyWordCount implements MapReduceJob {
	
	static final ONE = new LongWritable(1)
	static final DELIMITER = "/ * ' ? - \" \\ .  : _ [ ] < > \n , ; ( )"
	static final STOP_WORDS = new StopWords()
	
	def WordCountMapper = { key, value, context ->
		def line = value.toString()
		def tokenizer = new StringTokenizer(line, DELIMITER)
		
		tokenizer.each{ word ->
			if(!STOP_WORDS.isStopWord(word))
				context.write(word, ONE);
		}
	} as Mapper
	
	def LongSumReducer = { word, values, context ->
		Long sum = values.collect{ it.get() }.sum()
		context.write(word, sum)

	} as Reducer
	

	Class mapperClass() {
		WordCountMapper.class;
	}

	Class reducerClass() {
		LongSumReducer.class;
	}

	Class combiner() {
		LongSumReducer.class;
	}

	Class jarByClass() {
		GroovyWordCount.class;
	}

	Class mapOutputKeyClass() {
		Text.class;
	}

	Class mapOutputValueClass() {
		LongSumReducer.class;
	}

}
