package com.acadgild;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducer extends Reducer<TextPair, IntWritable, Text, IntWritable> {

	public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int NoOfArrests = 0;

		for (IntWritable arrests : values) {

			NoOfArrests += arrests.get();

		}

		context.write(key.getFirst(), new IntWritable(NoOfArrests));
	}
}
