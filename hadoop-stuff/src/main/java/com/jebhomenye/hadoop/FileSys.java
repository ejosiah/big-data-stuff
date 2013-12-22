package com.jebhomenye.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSys {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Path path = new Path("/user/hadoop");
		
		FileStatus status = hdfs.getFileStatus(path);
		
		print(status, hdfs);
	}
	
	private static void print(FileStatus status, FileSystem fs) throws FileNotFoundException, IOException{
		if(status.isDirectory()){
			System.out.println(status.getPath());
			for(FileStatus stat : fs.listStatus(status.getPath())){				
				print(stat, fs);
			}
		}else{
			System.out.println("\t" + status.getPath());
		}
	}
}
