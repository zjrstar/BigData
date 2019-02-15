package com.solr.learner;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestSolr {

  private SolrClient client;

  @Before
  public void setup() {
    String url = "http://localhost:8983/solr/solrexample";
    client = new HttpSolrClient.Builder(url).build();
  }

  @Test
  public void createIndex() throws IOException, SolrServerException {
    final File dir = new File("data");
    final File[] files = dir.listFiles();
    for (File file : files) {
      final SolrInputDocument document = new SolrInputDocument();
      document.addField("name", file.getName());
      document.addField("content", FileUtils.readFileToString(file, "utf-8"));
      document.addField("time", file.lastModified());
      client.add(document);
    }
    client.optimize();
    client.commit();
    client.close();
  }

  @Test
  public void createIndexBean() throws IOException, SolrServerException {
    final File dir = new File("data");
    final File[] files = dir.listFiles();
    List<Doc> list = new ArrayList<Doc>();
    for (File file : files) {
      final Doc doc = new Doc();
      doc.setName(file.getName());
      doc.setContent(FileUtils.readFileToString(file, "utf-8"));
      doc.setTime(file.lastModified());
//      final SolrInputDocument document = new SolrInputDocument();
//      document.addField("name", file.getName());
//      document.addField("content", FileUtils.readFileToString(file, "utf-8"));
//      document.addField("time", file.lastModified());
      list.add(doc);
    }
    client.addBeans(list.iterator());
    client.optimize();
    client.commit();
    client.close();
  }

  @Test
  public void search() throws IOException, SolrServerException {
    final SolrQuery query = new SolrQuery();
    query.setQuery("content:includes");
    final QueryResponse response = client.query(query);
    final SolrDocumentList results = response.getResults();
    final Iterator<SolrDocument> iterator = results.iterator();
    while (iterator.hasNext()) {
      final SolrDocument document = iterator.next();
      System.out.println(document.getFieldValue("name"));
    }
  }


}
