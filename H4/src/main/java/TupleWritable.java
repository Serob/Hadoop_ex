import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TupleWritable implements Writable {

    private Text word;
    private IntWritable count;

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
        word.readFields(in);
        count.readFields(in);
    }

    @Override
    public String toString() {
        return word.toString() +"\t" + count.toString();
    }
}
