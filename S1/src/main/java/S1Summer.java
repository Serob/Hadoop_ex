import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class S1Summer extends Reducer<CoupleWritable, IntWritable, IntWritable, CoupleWritable> {

    @Override
    public void reduce(CoupleWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write (new IntWritable(sum), (key));
    }
}
