package com.sentimentanalysis.task1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Comparator;

public class task1reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private PriorityQueue<UserInteraction> topUsers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Initialize a priority queue with a custom comparator to maintain the top 10 users
        topUsers = new PriorityQueue<>(10, Comparator.comparingInt(UserInteraction::getCount));
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        // Add the user and their interaction count to the priority queue
        topUsers.offer(new UserInteraction(key.toString(), sum));

        // If the size of the priority queue exceeds 10, remove the least engaged user
        if (topUsers.size() > 10) {
            topUsers.poll();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<UserInteraction> sortedUsers = new ArrayList<>(topUsers);
        Collections.sort(sortedUsers, (a, b) -> b.getCount() - a.getCount());

        for (UserInteraction user : sortedUsers) {
            context.write(new Text(user.getUserId()), new IntWritable(user.getCount()));
        }
    }

    private static class UserInteraction {
        private final String userId;
        private final int count;

        public UserInteraction(String userId, int count) {
            this.userId = userId;
            this.count = count;
        }

        public String getUserId() {
            return userId;
        }

        public int getCount() {
            return count;
        }
    }
}
