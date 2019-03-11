import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

public class SortMapper extends Mapper<Object, Text, Text, TupleWritable> {

    private static  class Container implements Comparable<Container>{

        private Text key;
        private TupleWritable value;

        Container(Text key, TupleWritable value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(Container o) {
            return value.compareTo(o.value);
        }
    }
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        List<Container> containers = new ArrayList<>();
        try {
            while (context.nextKeyValue()) {
                String[] elements =  context.getCurrentValue().toString().split("\\s+");
                //maybe check if there are exactly 3 elements
                Text firstWord = new Text(elements[0]);
                TupleWritable tuple = new TupleWritable(elements[1], Integer.parseInt(elements[2]));
                containers.add(new Container(firstWord, tuple));
            }

            containers.sort(Collections.reverseOrder());
            for(Container container : containers){
                context.write(container.key, container.value);
//                map(container.key, container.value, context);
            }

        } finally {
            cleanup(context);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        final IntWritable sameKey = new IntWritable(1);
//
//        context.write(sameKey, value);

        String[] elements = value.toString().split("\\s+");
        //maybe check if there are exactly 3 elements
        Text firstWord = new Text(elements[0]);
        TupleWritable tuple = new TupleWritable(elements[1], Integer.parseInt(elements[2]));
        context.write(firstWord, tuple);
    }
}
