import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;

public class WordMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    public static int searchableLength;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        TokenStream stream = analyzer.tokenStream(null, value.toString());

        stream.reset();
        while (stream.incrementToken()) {
            String token = stream.getAttribute(CharTermAttribute.class).toString();
            if(token.length() == searchableLength){
                context.write(new IntWritable(searchableLength), new IntWritable(1));
            }
        }
        stream.end();
        stream.close();

    }
}
