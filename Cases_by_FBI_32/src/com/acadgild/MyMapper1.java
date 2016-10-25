package com.acadgild;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper1 extends Mapper<Text, IntWritable, Text, IntWritable> {
	//private Text FBI_code = new Text();

	//private final static IntWritable one = new IntWritable(1);

	public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

		//String line = value.toString();
		//String crime_data[] = line.split(",");

		if ( key.equals(new Text("32"))) {
			Text key1=key;
			context.write(key1, value);
		}

		

	}
}