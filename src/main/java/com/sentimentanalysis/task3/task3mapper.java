package com.sentimentanalysis.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class task3mapper extends Mapper<Object, Text, Text, IntWritable> {

    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Text categoryHour = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // Ensure the input has 7 fields as expected
        if (fields.length == 7) {
            String productCategory = fields[2];  // Product category
            String transactionTimestamp = fields[6];  // Transaction timestamp

            try {
                // Parse the timestamp and extract the hour (HH)
                Date date = dateFormat.parse(transactionTimestamp);
                SimpleDateFormat hourFormat = new SimpleDateFormat("HH");
                String hour = hourFormat.format(date);

                // Create a composite key of ProductCategory@Hour
                String categoryHourKey = productCategory + "@" + hour;
                categoryHour.set(categoryHourKey);

                // Emit (ProductCategory@Hour, 1)
                context.write(categoryHour, one);
            } catch (ParseException e) {
                // Handle any parsing errors for invalid timestamps
                e.printStackTrace();
            }
        }
    }
}
