import org.apache.commons.io.IOExceptionList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.thirdparty.com.google.errorprone.annotations.ForOverride;

import java.io.IOException;

public class task2mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip the header
        if (line.startsWith("LogID") || line.startsWith("TransactionID")) {
            return;
        }

        String[] fields = line.split(',');

        //get from user_activity.csv
        if (fields.length == 5) {
            String activityType = fields[2]; //get activity type/interaction
            String productId = fields[3]; //get the product id
            if (activityType.equalsIgnoreCase("purchase")) { //looks for a purchase interactions based on the activity type
                outputKey.set(productId + ":purchase");
            } else {
                outputKey.set(productId + ":interaction"); //for other interactions if not purchase
            }
            context.write(outputKey, one);
        }
        
        // get from transactions.csv
        if (fields.length == 7) {
            String productCategory = fields[2];
            outputKey.set(productCategory + ":purchase"); //getting the product category
            context.write(outputKey, one);
        }

    }
}