import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;

public class WordMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String isSetStr = conf.get(HadoopDriver.PARAM_IS_SET);
        boolean isCountSet = Boolean.valueOf(isSetStr);


        StandardAnalyzer analyzer = new StandardAnalyzer();
        TokenStream stream = analyzer.tokenStream(null, value.toString());

        stream.reset();
        while (stream.incrementToken()) {
            String token = stream.getAttribute(CharTermAttribute.class).toString();
            if (isCountSet) {
                int count = Integer.valueOf(conf.get(HadoopDriver.PARAM_LENGTH));
                if (token.length() == count) {
                    context.write(new IntWritable(count), new IntWritable(1));
                }
            } else {
                context.write(new IntWritable(token.length()), new IntWritable(1));
            }
        }
        stream.end();
        stream.close();

    }
}
