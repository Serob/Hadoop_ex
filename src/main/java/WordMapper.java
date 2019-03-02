import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

import java.io.*;
import java.util.*;

public class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);

        Directory indexDirectory = new RAMDirectory();
        IndexWriter writer = new IndexWriter(indexDirectory, config);

        Document doc = new Document();
        FieldType type = new FieldType();
        type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        type.setStored(true);
        type.setStoreTermVectors(true);
        doc.add(new Field("value", value.toString(), type));
        doc.add(new StringField("key", key.toString(),  Field.Store.YES));

        writer.addDocument(doc);
        writer.close();

        IndexReader reader = DirectoryReader.open(indexDirectory);

        Terms terms = reader.getTermVector(0, "value");
        BytesRefIterator iterator = terms.iterator();
        BytesRef byteRef;
        while ((byteRef = iterator.next()) != null) {
            String word = byteRef.utf8ToString();
            int count = (int)reader.totalTermFreq(new Term("value", word)); //int is OK (I hope))
            context.write(new Text(word), new IntWritable(count));
        }

/*        //TermQuery
        Query tst = new TermQuery(new Term("value", "test"));

        //RegexpQuery
        RegexpQuery wordQuery = new RegexpQuery(new Term("value", "[a-z]"));

        //PhraseQuery
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.add(new Term("value", "test"), 0);
        PhraseQuery pq = builder.build();

        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs docs = searcher.search(wordQuery, 20);
        ScoreDoc[] hits = docs.scoreDocs;

        for (int i = 0; i < hits.length; ++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". " + d.get("key") + "\t" + d.get("value"));
        }*/

    }


}


