package com.acadgild;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import com.acadgild.MyMapper;

public class CasesNoByFBI_Code {

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "driver");
		job.setJarByClass(CasesNoByFBI_Code.class);
		//setting mapper class
		job.setMapperClass(MyMapper.class);
		//setting Reducer class
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//path to crimes.csv
		FileInputFormat.addInputPath(job, new Path(args[0]));		
		//output location
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);

		

	}

}
