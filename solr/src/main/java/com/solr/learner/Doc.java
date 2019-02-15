package com.solr.learner;

import org.apache.solr.client.solrj.beans.Field;

public class Doc {
  @Field
  private String name;
  @Field
  private String content;
  @Field
  private Long time;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public Long getTime() {
    return time;
  }

  public void setTime(Long time) {
    this.time = time;
  }
}
