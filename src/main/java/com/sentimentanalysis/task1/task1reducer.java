package com.sentimentanalysis.task1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class task1reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //the priorityqueue helps maintain the top 10 users, it will keep them sorted by sum
    private PriorityQueue<UserInteraction> topUsers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize a priority queue with a custom comparator to maintain the top 10 users
        topUsers = new PriorityQueue<>(10, Comparator.comparingInt(UserInteraction::getCount));
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0; //first, sum up the total interaction count for each user
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Add the user and their interaction count to the priority queue with the new custom class
        topUsers.offer(new UserInteraction(key.toString(), sum));

        // If the size of the priority queue exceeds 10, remove the least engaged user
        if (topUsers.size() > 10) {
            topUsers.poll();
        }
    }

    @Override //cleanup class will yields the top 10 users
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //convert the queue to a list and is sorted in descedning order by the interaction count
        List<UserInteraction> sortedUsers = new ArrayList<>(topUsers);
        Collections.sort(sortedUsers, (a, b) -> b.getCount() - a.getCount());

        //the sorted list is written to the context
        for (UserInteraction user : sortedUsers) {
            context.write(new Text(user.getUserId()), new IntWritable(user.getCount()));
        }
    }

    //this custom class helps with getting the details of the user and count
    private static class UserInteraction {
        private final String userId;
        private final int count;

        //constructor functions
        public UserInteraction(String userId, int count) {
            this.userId = userId;
            this.count = count;
        }

        //get functions
        public String getUserId() {
            return userId;
        }

        public int getCount() {
            return count;
        }
    }
}
