package com.jebhomenye.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
	
	
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
	
	public static void main(String[] args) throws Exception{
		runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
	}
	
	public static void runJob(String[] input, String output) throws Exception{
		Configuration configuration = new Configuration();
		
		Job job = new Job(configuration);
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(LongSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);	
		
		Path outputPath = new Path(output);
		
		FileInputFormat.setInputPaths(job, Arrays.toString(input).replace("[", "").replace("]", ""));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(configuration).delete(outputPath, true);
		job.waitForCompletion(true);
	}
}
