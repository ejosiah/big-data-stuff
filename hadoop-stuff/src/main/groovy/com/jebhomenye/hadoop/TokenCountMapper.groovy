package com.jebhomenye.hadoop

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Mapper.Context

class TokenCountMapper extends Mapper<Object, Text, Text, LongWritable> {
	
	static final ONE = new LongWritable(1)
	static final DELIMITER = "/ * ' ? - \" \\ .  : _ [ ] < > \n , ; ( )"
	static final STOP_WORDS = new StopWords()

	void map(Object key, Text  value, Context context) {
		def line = value.toString()
		def tokenizer = new StringTokenizer(line, DELIMITER)
		
		tokenizer.each{ word ->
			if(!STOP_WORDS.isStopWord(word)){
				context.write(new Text(word.toLowerCase()), ONE);
			}
		}
	}

}
