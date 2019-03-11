import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.LinkedList;
import java.util.Locale;

public class WordMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        StringBuilder buffer = new StringBuilder();
        BreakIterator boundary = BreakIterator.getSentenceInstance(new Locale("ru", "RU"));
        try {
            while (context.nextKeyValue()) {
                if(buffer.length() > 0){
                    buffer.append(" ");
                }
                buffer.append(context.getCurrentValue().toString());
                String sentence = getFirstSentence(boundary, buffer);
                if(sentence != null){
                    map(context.getCurrentKey(), new Text(sentence), context);
                }
            }
            //map on tail
            if(buffer.length() > 0){
                //considering that all remaining part is a sentence.
                map(context.getCurrentKey(), new Text(buffer.toString()), context);
            }

        } finally {
            cleanup(context);
        }
    }

    /**
     * Return first sentence if there are more than one in the provided buffer, otherwise - return {@code null}.
     * <br>
     * NOTE: changes the state of the buffer: after returning not {@code null} value will remove corresponding data from the buffer.
     * @param boundary Initialized object of {@link BreakIterator} in some locale.
     * @param buffer {@link StringBuilder} object, holding all sentences.
     * @return A sentence, if there are more than one in the provided buffer {@link StringBuilder}
     */
    private static String getFirstSentence(BreakIterator boundary, StringBuilder buffer){
        boundary.setText(buffer.toString());
        int concreteEnd = boundary.last();
        int start = boundary.first();
        int end = boundary.next();

        if(concreteEnd != end && end != BreakIterator.DONE){
            String sentence = buffer.substring(start, end);
            buffer.replace(start, end, ""); //remove the sentence from the buffer
            return sentence;
        }
        return null;
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StandardAnalyzer analyzer = new StandardAnalyzer();
        addToContext(value.toString(), analyzer, context);
    }

    private void addToContext(String value, Analyzer analyzer, Context context)
            throws IOException, InterruptedException {

        TokenStream stream = analyzer.tokenStream(null, value);
        LinkedList<String> couple = new LinkedList<>(); // two words following each other

        stream.reset();
        while (stream.incrementToken()) {
            String token = stream.getAttribute(CharTermAttribute.class).toString();
            couple.add(token);
            if (couple.size() > 2) {
                couple.poll();
            }
            if (couple.size() == 2) {
                String el1 = couple.peekFirst();
                String el2 = couple.peekLast();
                context.write(new Text(el1), new Text(el2));
            }
        }
        stream.end();
        stream.close();
    }
}


