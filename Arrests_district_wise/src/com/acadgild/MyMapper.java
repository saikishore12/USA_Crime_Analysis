package com.acadgild;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
	private static TextPair District_data = new TextPair();
	private final static IntWritable one = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String crime_data[] = line.split(",");
		
		if (crime_data.length > 22) {
			//Getting District
			Text District=new Text(crime_data[11]);
			//Arrest info of district
			Text Arrests=new Text(crime_data[8]);
			District_data.set(District,Arrests);
		}
		//verifying the arrest status for a district 
		//if true counting it as 1
		if (District_data.getSecond().equals(new Text("TRUE")))
			context.write(District_data, one);

	}
}