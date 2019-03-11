import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SortReducer extends Reducer<IntWritable, Text, Text, TupleWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeSet<Container> containers = new TreeSet<>();
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
        }
    }

    private static  class Container implements Comparable{

        private Text key;
        private TupleWritable value;

        Container(Text key, TupleWritable value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(Object o) {
            return value.compareTo(((Container)o).value);
        }
    }

}
