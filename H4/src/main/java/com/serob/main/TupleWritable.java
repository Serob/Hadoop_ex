package com.serob.main;

import com.serob.compare.TupleWritableComparator;
import com.sun.istack.NotNull;
import org.apache.hadoop.io.*;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TupleWritable implements Writable, WritableComparable<TupleWritable> {

    private Text word;
    private IntWritable count;

    public TupleWritable() {
    }

    public TupleWritable(Text word, IntWritable count) {
        this.word = word;
        this.count = count;
    }

    public TupleWritable(Text word, int count) {
        this(word, new IntWritable(count));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = new Text (in.readUTF());
        count = new IntWritable(in.readInt());

//        word.readFields(in);
//        count.readFields(in);
    }

    @Override
    public String toString() {
        return word.toString() +"\t" + count.toString();
    }

    @Override
    public int compareTo(TupleWritable tWritable) {
        return count.compareTo(tWritable.count);
    }

//    static {  // register this comparator
//        WritableComparator.define(TupleWritable.class, new TupleWritableComparator());
//    }
}
