package org.ajames.lucenetermvectors;

import java.util.List;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Paths;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;

public class App {

    final String indexPath = "./index";
    final String fieldName = "body";
    final String content = "Lorem - ipsum dolor, sit amet ipsum";
    final String searchTerm = "ipsum";

    public static void main(String[] args) throws IOException, ParseException {
        App app = new App();
        app.buildIndex();
        app.doSearch();
        app.getDynamicOffsets(app.fieldName, app.searchTerm);
        app.getTokens();
    }

    private void buildIndex() throws IOException {
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        // using SimpleTextCodec for human-readable indexes (poor performance!):
        iwc.setOpenMode(OpenMode.CREATE).setCodec(new SimpleTextCodec());
        try ( IndexWriter writer = new IndexWriter(dir, iwc)) {
            indexDoc(writer);
        }
    }

    // Create one document with 2 fields (and ID field and a content body):
    private void indexDoc(IndexWriter writer) throws IOException {
        Document doc = new Document();

        StringField idField = new StringField("doc_id_field", "doc one", Field.Store.YES);
        doc.add(idField);

        FieldType fieldType = new FieldType();
        fieldType.setIndexOptions(IndexOptions.DOCS);
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorPositions(true);
        fieldType.setStoreTermVectorOffsets(true);

        doc.add(new Field(fieldName, content, fieldType));
        writer.addDocument(doc);
    }

    // performs a basic search and uses term vector index to  print offsets:
    private void doSearch() throws IOException, ParseException {
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        try ( DirectoryReader dirReader = DirectoryReader.open(dir)) {
            IndexSearcher indexSearcher = new IndexSearcher(dirReader);
            Analyzer analyzer = new StandardAnalyzer();

            QueryParser parser = new QueryParser(fieldName, analyzer);
            Query query = parser.parse(searchTerm);
            System.out.println();
            System.out.println("Search term: [" + searchTerm + "]");
            System.out.println("Parsed query: [" + query.toString() + "]");

            ScoreDoc[] hits = indexSearcher.search(query, 100).scoreDocs;
            for (ScoreDoc hit : hits) {
                BigDecimal score = new BigDecimal(String.valueOf(hit.score))
                        .setScale(3, RoundingMode.HALF_EVEN);
                Document hitDoc = indexSearcher.doc(hit.doc);
                System.out.println();
                System.out.println("Found:");
                System.out.println(String.format("%s - %s",
                        String.format("%7.3f", score),
                        String.format("%-10s", hitDoc.get("doc_id_field"))));
                getIndexedOffsets(hit.doc, fieldName, searchTerm);
            }
        }
    }

    // use term vector index to get offset(s) for a term in a field in a document:
    private void getIndexedOffsets(int docID, String fieldName, String searchTerm) throws IOException {
        try ( IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
            Terms terms = reader.getTermVector(docID, fieldName);
            TermsEnum termsEnum = terms.iterator();
            BytesRef bytesRef = new BytesRef(searchTerm);
            if (termsEnum.seekExact(bytesRef)) {
                PostingsEnum pe = termsEnum.postings(null, PostingsEnum.OFFSETS);
                pe.nextDoc();
                int freq = pe.freq();
                for (int i = 0; i < freq; i++) {
                    pe.nextPosition();
                    if (pe.startOffset() >= 0) {
                        printOffset(pe.startOffset(), pe.endOffset());
                    }
                }
            }
        }
    }

    // generates term vector data on-the-fly for offset information:
    private void getDynamicOffsets(String field, String searchTerm) throws IOException {
        Analyzer analyzer = new StandardAnalyzer();
        TokenStream ts = analyzer.tokenStream(field, content);
        CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAttr = ts.addAttribute(OffsetAttribute.class);

        try {
            ts.reset();
            System.out.println();
            System.out.println("token: " + searchTerm);
            while (ts.incrementToken()) {
                if (searchTerm.equals(charTermAttr.toString())) {
                    printOffset(offsetAttr.startOffset(), offsetAttr.endOffset());
                }
            }
            ts.end();
        } finally {
            ts.close();
        }
    }

    private void printOffset(int start, int end) {
        System.out.println(String.format("  > offset: %s-%s", start, end));
    }

    // lists the tokens in an index:
    private void getTokens() throws IOException, ParseException {
        Directory dir = FSDirectory.open(Paths.get(indexPath));
        try ( DirectoryReader dirReader = DirectoryReader.open(dir)) {
            List<LeafReaderContext> list = dirReader.leaves();
            System.out.println();
            for (LeafReaderContext lrc : list) {
                Terms terms = lrc.reader().terms(fieldName);
                TermsEnum termsEnum = terms.iterator();
                BytesRef term;
                while ((term = termsEnum.next()) != null) {
                    System.out.println(term.utf8ToString());
                }
            }
        }
    }

}
