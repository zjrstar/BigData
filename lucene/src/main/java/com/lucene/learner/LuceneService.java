package com.lucene.learner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;

@Service
public class LuceneService {

  public void createIndex(String indexPath, String dataPath) throws IOException {
    //打开文件路径
    Directory dir = FSDirectory.open(Paths.get(indexPath, new String[0]));
    IndexWriterConfig conf = new IndexWriterConfig(new SmartChineseAnalyzer());
    //设置写入模式
    conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    IndexWriter writer = new IndexWriter(dir, conf);
    File data = new File(dataPath);
    Collection<File> files = FileUtils.listFiles(data, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
    //创建一块内容，目前每50个document的索引写一次磁盘，清空内存，避免内存占用过大
    RAMDirectory ramDirectory = new RAMDirectory();
    IndexWriterConfig ramConf = new IndexWriterConfig(new SmartChineseAnalyzer());
    IndexWriter ramWriter = new IndexWriter(ramDirectory, ramConf);
    int count = 0;
    for (File file : files) {
      HTMLBean bean = HTMLUtils.htmlParse(file);
      if (bean == null) {
        continue;
      }
      Document document = new Document();
//      document.add(new TextField("content", FileUtils.readFileToString(file), Field.Store.YES));
//      document.add(new StringField("name", file.getName(), Field.Store.YES));
//      document.add(new LongField("time", file.lastModified(), Field.Store.YES));
      document.add(new TextField("content", bean.getContent(), Field.Store.YES));
      document.add(new StringField("title", bean.getTitle(), Field.Store.YES));
      document.add(new StringField("url", bean.getUrl(), Field.Store.YES));
//      writer.addDocument(document);
      ramWriter.addDocument(document);
      if (count == 50) {
        ramWriter.commit();
        ramWriter.close();
        writer.addIndexes(ramDirectory);
        ramDirectory = new RAMDirectory();
        ramConf = new IndexWriterConfig(new SmartChineseAnalyzer());
        ramWriter = new IndexWriter(ramDirectory, ramConf);
      }
      count++;
    }
    ramWriter.commit();
    ramWriter.close();
    writer.addIndexes(ramDirectory);
    writer.commit();
    writer.close();
  }

  public void search() throws IOException, ParseException {
    SimpleFSDirectory directory = new SimpleFSDirectory(Paths.get("index"));
    DirectoryReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);
    QueryParser parser = new QueryParser("content", new StandardAnalyzer());
    Query query = parser.parse("java");
    TopDocs search = searcher.search(query, 10);
    ScoreDoc[] scoreDocs = search.scoreDocs;
    for (ScoreDoc doc : scoreDocs) {
      Document document = reader.document(doc.doc);
      System.out.println(document.get("name"));
    }
  }
}




































