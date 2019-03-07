package com.serob.main;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.IOException;
import java.util.LinkedList;

public class WordMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StandardAnalyzer analyzer = new StandardAnalyzer();

        String[] sentences = value.toString().split("\\.");
        for (String s : sentences) {
            addToContext(s, analyzer, context);
        }

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


