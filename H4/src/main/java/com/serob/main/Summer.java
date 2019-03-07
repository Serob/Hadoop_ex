package com.serob.main;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Summer extends Reducer<Text, Text, Text, TupleWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<Text, Integer> counts = new HashMap<>();
        int max = 0;
        Text word_max = null;
        for (Text val : values) {
            counts.putIfAbsent(val, 0);
            int count = counts.get(val);
            counts.put(val, ++count);
            if (count > max){
                max = count;
                word_max = new Text(val);
            }
        }
        context.write(key, new TupleWritable(word_max, max));
    }
}
