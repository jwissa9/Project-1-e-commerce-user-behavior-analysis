package com.sentimentanalysis.task3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class task3reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // Sum all transaction counts for this key (ProductCategory@Hour)
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Set the total count as the result and write it to the context
        result.set(sum);
        context.write(key, result);
    }
}
