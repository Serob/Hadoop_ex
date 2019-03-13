import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TupleWritable implements WritableComparable<TupleWritable> {

    private Text firstWord;
    private Text word;
    private IntWritable count;

    //MUST exist to be called during auto-sort process !!!
    /**
     * Default constructor to be called by Hadoop reflection on keys auto-sort process.
     * <br>
     * Initialize readable parameters to non-null values, otherwise {@link NullPointerException}
     * will be thrown on {@link WritableComparator}'s compare method.
     */
    public TupleWritable(){
        word = new Text();
        count = new IntWritable();
    }

    public TupleWritable(Text word, IntWritable count) {
        this.word = word;
        this.count = count;
    }

    /**
     * Creat full-line object describing first word, the word after it, and the repetition count
     * (use this to achieve right {@code equals()} result).
     * @param firstWord The word which other word is following; type {@link Text}. Not a readable value.
     * @param word The word which is following the {@code firstWord} param: type {@link Text}
     * @param count The count of {@code word} param to be following {@code firstWord} param {@code
     * int}
     */
    public TupleWritable(Text firstWord, Text word, int count) {
        this.firstWord = firstWord;
        this.word = word;
        this.count = new IntWritable(count);
    }

    /**
     * Creat full-line object describing first word, the word after it, and the repetition count
     * (use this to achieve right {@code equals()} result).
     * @param firstWord The word which other word is following; type {@link String}. Not a readable value.
     * @param word The word which is following the {@code firstWord} param: type {@link String}
     * @param count The count of {@code word} param to be following {@code firstWord} param: type {@code
     * int}
     */
    public TupleWritable(String firstWord, String word, int count) {
        this(new Text(firstWord), new Text(word), count);
    }

    public TupleWritable(Text word, int count) {
        this(word, new IntWritable(count));
    }

    public TupleWritable(String word, int count) {
        this(new Text(word), new IntWritable(count));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        word.write(out);
        count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //DON'T use these 2 lines if sort have to be used: this will lead EOF Exception
//        word = new Text (in.readUTF());
//        count = new IntWritable(in.readInt());
        word.readFields(in);
        count.readFields(in);
    }

    @Override
    public int compareTo(TupleWritable tWritable) {
        return count.compareTo(tWritable.count);
    }

    @Override
    public String toString() {
        return word.toString() + "\t" + count.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TupleWritable)) return false;
        TupleWritable that = (TupleWritable) o;
        return Objects.equals(firstWord, that.firstWord) &&
                Objects.equals(word, that.word) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstWord, word, count);
    }

    static {  // register this comparator
        WritableComparator.define(TupleWritable.class, new Comparator());
    }

    /**
     * Comparator to be used on auto-sort (reverse order).
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(TupleWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TupleWritable tw1 = (TupleWritable)a;
            TupleWritable tw2 = (TupleWritable)b;
            return super.compare(tw1, tw2) * -1; //reverse  order
        }
    }
}
