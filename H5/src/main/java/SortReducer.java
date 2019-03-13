import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SortReducer extends Reducer<TupleWritable, Text, Text, TupleWritable> {

    @Override
    protected void reduce(TupleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
/*        TreeSet<Container> containers = new TreeSet<>();
        List<Container> vle = new ArrayList<>();

        values.forEach(v -> {
            String[] elements = v.toString().split("\\s+");
            //maybe check if there are exactly 3 elements
            Text firstWord = new Text(elements[0]);
            TupleWritable tuple = new TupleWritable(elements[1], Integer.parseInt(elements[2]));

            containers.add(new Container(firstWord, tuple));
        });

        for(Container c : containers){
            context.write(c.key, c.value);
        }*/

        //actually values must contain ONE element
        for (Text value : values) {
            context.write(value, key);
        }
    }

}
