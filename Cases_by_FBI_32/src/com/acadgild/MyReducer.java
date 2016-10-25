package com.acadgild;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int NoOfCases = 0;

		for (IntWritable cases : values) {

			NoOfCases += cases.get();
			
		}
		
		
		context.write(key, new IntWritable(NoOfCases));
		
	}
}
