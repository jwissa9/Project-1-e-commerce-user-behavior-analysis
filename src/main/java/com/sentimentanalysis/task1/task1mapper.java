package com.sentimentanalysis.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class task1mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1); //set for the count
    private Text userID = new Text(); //set for id

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.startsWith("LogID")) { //skip the header
            return;
        }

        String[] fields = line.split(",");
        if (fields.length > 1) {
            userID.set(fields[1]); //get the user id
            context.write(userID, one); //set the key-value pair with id and counter
        }

    }
}