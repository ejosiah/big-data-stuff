package com.jebhomenye.hadoop;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("all")
public class MapReduceJobRunner {
	
	public static void main(String[] args) throws Exception{
		String className = args[0];
		Class<? extends MapReduceJob> clazz = (Class<? extends MapReduceJob>) Class.forName(className);
		runJob(clazz.newInstance(), Arrays.copyOfRange(args, 1, args.length - 1), args[args.length - 1]);
	}
	
	public static void runJob(MapReduceJob mapReduceJob, String[] input, String output) throws Exception{
		Configuration configuration = new Configuration();
		
		Job job = new Job(configuration);
		
		job.setJarByClass(mapReduceJob.jarByClass());
		job.setMapperClass(mapReduceJob.mapperClass());
		job.setReducerClass(mapReduceJob.reducerClass());
		
		if(mapReduceJob.combiner() != null){
			job.setCombinerClass(mapReduceJob.combiner());
		}
		
		job.setMapOutputKeyClass(mapReduceJob.mapOutputKeyClass());
		job.setMapOutputValueClass(mapReduceJob.mapOutputValueClass());	
		
		Path outputPath = new Path(output);
		
		FileInputFormat.setInputPaths(job, Arrays.toString(input).replace("[", "").replace("]", ""));
		FileOutputFormat.setOutputPath(job, outputPath);
		
		outputPath.getFileSystem(configuration).delete(outputPath, true);
		job.waitForCompletion(true);
	}
}
