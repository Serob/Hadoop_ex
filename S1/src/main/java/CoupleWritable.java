import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CoupleWritable implements WritableComparable<CoupleWritable> {

    private Text firstWord;
    private Text secondWord;
    private IntWritable count;

    public CoupleWritable(){
        firstWord = new Text();
        secondWord = new Text();
        count = new IntWritable();
    }

    public CoupleWritable(Text firstWord, Text secondWord, IntWritable count){
        this.firstWord = firstWord;
        this.secondWord = secondWord;
        this.count = count;
    }

    public CoupleWritable(Text firstWord, Text secondWord){
        this(firstWord, secondWord, new IntWritable(1));
    }

    public CoupleWritable(String firstWord, String secondWord, int count){
        this.firstWord = new Text(firstWord);
        this.secondWord = new Text(secondWord);
        this.count = new IntWritable(count);
    }

    public CoupleWritable(String firstWord, String secondWord){
        this(firstWord, secondWord, 1);
    }

    public void addCount(int addition){
        int oldCount = this.count.get();
        int total = oldCount + addition;
        count.set(total);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        firstWord.write(out);
        secondWord.write(out);
//        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstWord.readFields(in);
        secondWord.readFields(in);
//        count.readFields(in);
    }

    @Override
    //Doesn't work without implementing comparable
    public int compareTo(CoupleWritable o) {
        //0 means created form default constructor and
        //without this check always equal
        if(count.get() != 0 || o.count.get() != 0){
            return count.compareTo(o.count);
        }
        return toString().compareTo(o.toString());
    }

    @Override
    public String toString() {
        return firstWord.toString() + "\t" + secondWord.toString();
    }

   /* static {  // register this comparator
        WritableComparator.define(CoupleWritable.class, new Comparator());
    }*/

    /**
     * Comparator to be used on auto-sort (reverse order).
     */
    /*public static class Comparator extends WritableComparator{
        public Comparator() {
            super(CoupleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CoupleWritable obj1 = (CoupleWritable)a;
            CoupleWritable obj2 = (CoupleWritable)b;
            return super.compare(obj1, obj2) * -1; //reverse  order
        }
    }*/
}
