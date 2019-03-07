package com.serob.compare;

import com.serob.main.TupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TupleValuePartitioner extends Partitioner<Text, TupleWritable> {

    @Override
    public int getPartition(Text text, TupleWritable tupleWritable, int numPartitions) {
        return text.hashCode() % numPartitions;
    }
}
