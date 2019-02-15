package com.lucene.learner;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Controller
public class LuceneController {

  @Autowired
  private LuceneService lucene;

  @RequestMapping(method = RequestMethod.GET, value = "/index")
  public String index() {
    return "index";
  }

  @RequestMapping(method = RequestMethod.GET, value = "/create")
  public String createIndex() {
    File file = new File("/Users/jerry/index");
    try {
      if (file.exists()) {
        FileUtils.deleteDirectory(file);
        file.mkdirs();
      }
      lucene.createIndex("/Users/jerry/index", "/Users/jerry/data/www.bjsxt.com");
    } catch (IOException e) {
      e.printStackTrace();
    }
    return "index";
  }

  @RequestMapping("/search")
  public String search(String keyWord, @RequestParam(defaultValue = "1") Integer currentPageNum,
                       @RequestParam(defaultValue = "3") Integer pageSize, Model model) {
    try {
      FSDirectory directory = FSDirectory.open(Paths.get("/Users/jerry/index", new String[0]));
      DirectoryReader reader = DirectoryReader.open(directory);
      IndexSearcher searcher = new IndexSearcher(reader);
      MultiFieldQueryParser queryParser = new MultiFieldQueryParser(new String[] {"content", "title", "url"}, new SmartChineseAnalyzer());
      Query query = queryParser.parse(keyWord);
      TopDocs topDocs = searcher.search(query, 10);
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      System.out.println(topDocs.totalHits);
      List<HTMLBean> list = new ArrayList<HTMLBean>();
      for (int i = (currentPageNum - 1) * 10; i < Math.min(currentPageNum * 10, topDocs.totalHits); i++) {
        int doc = scoreDocs[i].doc;
        Document document = reader.document(doc);
        String title = document.get("title");
        String content = document.get("content");
        String url = document.get("url");
        SimpleHTMLFormatter formatter = new SimpleHTMLFormatter("<font color=\"red\">", "</font>");
        QueryScorer scorer = new QueryScorer(query, "content");
        Highlighter highlighter = new Highlighter(formatter, scorer);
        String hiTitle = highlighter.getBestFragment(new SmartChineseAnalyzer(), "title", title);
        String hiContent = highlighter.getBestFragments(new SmartChineseAnalyzer().tokenStream("content", content), content, 3, "...");
        HTMLBean bean = new HTMLBean();
        bean.setContent(hiContent);
        bean.setTitle(hiTitle);
        bean.setUrl(url);
        list.add(bean);
      }
      PageUtil<HTMLBean> page = new PageUtil<HTMLBean>((int) topDocs.totalHits, currentPageNum);
      page.setList(list);
//      model.addAttribute("list", list);
      //获取当前页
      model.addAttribute("pageNum", currentPageNum);
      //获取一页显示的条数
      model.addAttribute("pageSize", pageSize);
      model.addAttribute("page", page);
      model.addAttribute("total", topDocs.totalHits);
      model.addAttribute("keyWord", keyWord);
      return "index";
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    } catch (InvalidTokenOffsetsException e) {
      e.printStackTrace();
    }
    return "index";
  }
}
