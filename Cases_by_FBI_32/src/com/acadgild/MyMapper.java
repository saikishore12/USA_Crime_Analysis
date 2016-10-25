package com.acadgild;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text FBI_code = new Text();

	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String crime_data[] = line.split(",");

		if (crime_data.length > 15) {
			FBI_code.set(crime_data[14]);
		}

		context.write(FBI_code, one);

	}
}