import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class task2reducer extends Reducer<Text, IntWritable, Text, Text> {

    //start to count the purchases and other interactions
    private Map<String, Integer> interactionCount = new HashMap<>();
    private Map<String, Integer> purchaseCount = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get(); //sums the values for each key
        }

        String[] categoryInfo = key.toString().split(":"); //break apart the key
        String productCategory = categoryInfo[0]; //get the category
        String activityType = categoryInfo[1]; //get the activity type

        //differentiates between purchase and other interactions
        if (activityType.equals("purchase")) { //if type is purchase, count the purchases in each category
            purchaseCount.put(productCategory, purchaseCount.getOrDefault(productCategory, 0) + sum);
        } else { //else, then count for other interactions
            interactionCount.put(productCategory, interactionCount.getOrDefault(productCategory, 0) + sum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Integer> entry : interactionCount.entrySet()) {
            String category = entry.getKey();
            int interactions = entry.getValue();
            int purchases = purchaseCount.getOrDefault(category, 0);
            double conversionRate = purchases / (double) interactions;
            context.write(new Text(category), new Text("Conversion Rate: " + conversionRate));
        }
    }
}