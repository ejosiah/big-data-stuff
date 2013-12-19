package com.jebhomenye.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCount {
	
	
	private static class WordCountMapper extends Mapper<Object, Text, Text, LongWritable>{
		
		private static final LongWritable ONE = new LongWritable(1);
		private static final String DELIMITER = "/ * ' ? - \" \\ .  : _ [ ] < > \n , ; ( )";
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokinizer = new StringTokenizer(value.toString(), DELIMITER);
			
			while(tokinizer.hasMoreTokens()){
				Text word = new Text(tokinizer.nextToken());
				context.write(word, ONE);
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
	
	public static void main(String[] args) throws Exception{
		
		Configuration configuration = new Configuration();
		
		Job job = new Job(configuration);
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
	}
}
