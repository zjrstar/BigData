package com.lucene.learner;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.Source;
import net.htmlparser.jericho.TextExtractor;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class HTMLUtils {

  public static HTMLBean htmlParse(File file) throws IOException {
//    File file = new File("/Users/jerry/data/www.bjsxt.com/index.html");
    Source source = new Source(file);
    Element title = source.getFirstElement("title");
    TextExtractor textExtractor = source.getTextExtractor();
    HTMLBean bean = new HTMLBean();
    if (title==null) return null;
    bean.setTitle(title.getTextExtractor().toString());
    bean.setContent(textExtractor.toString());
    String path = file.getAbsolutePath();
    String url = "http://" + path.substring(path.indexOf("www.bjsxt.com")).replace("\\","/");
    bean.setUrl(url);
//    System.out.println(title.getTextExtractor().toString());
//    System.out.println(textExtractor.toString());
    return bean;
  }
}
