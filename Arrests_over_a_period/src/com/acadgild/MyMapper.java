package com.acadgild;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static TextPair District_data = new TextPair();
	private final static IntWritable one = new IntWritable(1);

	@SuppressWarnings("deprecation")
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String crime_data[] = line.split(",");

		if (crime_data.length > 22) {
			// Getting District
			Text Date = new Text(crime_data[2]);
			// Arrest info of district
			Text Arrests = new Text(crime_data[8]);
			District_data.set(Date, Arrests);
			
			// verifying the arrest status for the time duration
			// if true counting it as 1
			SimpleDateFormat df = new SimpleDateFormat("dd/mm/yy");
			try {
				Date Start_date = df.parse(crime_data[2]);
				if (Start_date.compareTo(new Date("01/10/2014")) > 0
						&& Start_date.compareTo(new Date("01/10/2015")) < 0) {
					if (District_data.getSecond().equals(new Text("TRUE")))
						context.write(new Text("count"), one);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}