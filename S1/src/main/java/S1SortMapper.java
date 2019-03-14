import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class S1SortMapper extends Mapper<Object, Text, IntWritable, CoupleWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] elements = value.toString().split("\\s+");
        int count = Integer.parseInt(elements[0]);
        CoupleWritable couple = new CoupleWritable(elements[1], elements[2]);
        context.write(new IntWritable(count), couple);
    }
}
