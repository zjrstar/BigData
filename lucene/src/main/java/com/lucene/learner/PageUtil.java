package com.lucene.learner;

import java.util.List;

public class PageUtil<T> {

  private int totalCount;//总数
  private int pageSize = 10;//每页显示数量
  private int currpageNum;//当前页
  private int pageCount;//总页数
  private int prePage;//上一页
  private int nextPage;//下一页
  private boolean hasPrePage;//是否有上一页
  private boolean hasNextPage;//是否有下一页
  private int firstPage;//第一页
  private int lastPage;//最后一页
  private int currentcount;//当前从第多少条数据开始显示
  private List<T> list;

  public List<T> getList() {
    return list;
  }

  public void setList(List<T> list) {
    this.list = list;
  }

  public PageUtil() {}

  public PageUtil(int totalCount, int pageNum) {
    this.totalCount = totalCount;
    this.currpageNum = pageNum;
    this.pageCount = (int) Math.ceil(1.0 * totalCount / pageSize);
    this.currentcount = (pageCount - 1) * pageSize;
    if (pageNum > 1) {  //判断是不是第一页
      /*--不是第一页 则有上一页 ，也有第一页--*/
      hasPrePage = true;
      prePage = pageNum - 1;
      firstPage = 1;
    }
    if (pageNum < pageCount) {//判断是不是最后一页
      /*--不是最后一页 则有上一页 ，也有最后一页--*/
      hasNextPage = true;
      nextPage = pageNum + 1;
      lastPage = pageCount;
    }
  }

  public int getTotalCount() {
    return totalCount;
  }

  public void setTotalCount(int totalCount) {
    this.pageCount = (int) Math.ceil(1.0 * totalCount / pageSize);
    if (this.currpageNum < 1) {
      this.currpageNum = 1;
    }
    this.currentcount = (currpageNum - 1) * pageSize;
    if (currpageNum > 1) {  //判断是不是第一页
      /*--不是第一页 则有上一页 ，也有第一页--*/
      hasPrePage = true;
      prePage = currpageNum - 1;
      firstPage = 1;
    }
    if (currpageNum < pageCount) {//判断是不是最后一页
      /*--不是最后一页 则有上一页 ，也有最后一页--*/
      hasNextPage = true;
      nextPage = currpageNum + 1;
      lastPage = pageCount;
    }
    this.totalCount = totalCount;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public int getPrePage() {
    return prePage;
  }

  public void setPrePage(int prePage) {
    this.prePage = prePage;
  }

  public int getNextPage() {
    return nextPage;
  }

  public void setNextPage(int nextPage) {
    this.nextPage = nextPage;
  }

  public boolean isHasPrePage() {
    return hasPrePage;
  }

  public void setHasPrePage(boolean hasPrePage) {
    this.hasPrePage = hasPrePage;
  }

  public boolean isHasNextPage() {
    return hasNextPage;
  }

  public void setHasNextPage(boolean hasNextPage) {
    this.hasNextPage = hasNextPage;
  }

  public int getFirstPage() {
    return firstPage;
  }

  public void setFirstPage(int firstPage) {
    this.firstPage = firstPage;
  }

  public int getLastPage() {
    return lastPage;
  }

  public void setLastPage(int lastPage) {
    this.lastPage = lastPage;
  }

  public int getCurrpageNum() {
    return currpageNum;
  }

  public void setCurrpageNum(int currpageNum) {
    this.currpageNum = currpageNum;
  }

  public int getPageCount() {
    return pageCount;
  }

  public void setPageCount(int pageCount) {
    this.pageCount = pageCount;
  }

  public int getCurrentcount() {
    return currentcount;
  }

  public void setCurrentcount(int currentcount) {
    this.currentcount = currentcount;
  }
}
