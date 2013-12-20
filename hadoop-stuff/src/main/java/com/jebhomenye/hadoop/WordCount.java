package com.jebhomenye.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount implements MapReduceJob {
	
	
	private static class WordCountMapper extends Mapper<Object, Text, Text, LongWritable>{
		
		private static final LongWritable ONE = new LongWritable(1);
		private static final String DELIMITER = "/ * ' ? - \" \\ .  : _ [ ] < > \n , ; ( )";
		private static final StopWords STOP_WORDS = new StopWords();
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			StringTokenizer tokinizer = new StringTokenizer(line, DELIMITER);
			
			while(tokinizer.hasMoreTokens()){
				String token = tokinizer.nextToken();
				
				if(!STOP_WORDS.isStopWord(token)){
					Text word = new Text(token);
					context.write(word, ONE);
				}
			}
		}
		
	}
	
	private static class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		protected void reduce(Text word, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for(LongWritable value : values){
				sum += value.get();
			}
			context.write(word, new LongWritable(sum));
		}

	}

	public Class<WordCountMapper> mapperClass() {
		return WordCountMapper.class;
	}

	public Class<LongSumReducer> reducerClass() {
		return LongSumReducer.class;
	}

	public Class<LongSumReducer> combiner() {
		return LongSumReducer.class;
	}

	public Class<WordCount> jarByClass() {
		return WordCount.class;
	}

	public Class<Text> mapOutputKeyClass() {
		return Text.class;
	}

	public Class<LongWritable> mapOutputValueClass() {
		return LongWritable.class;
	}
}
