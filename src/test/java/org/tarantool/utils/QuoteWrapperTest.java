package org.tarantool.utils;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class QuoteWrapperTest {

  @Test
  public void test(){
    String s = QuoteWrapper.addQuotesForSpaces("select * from \"test\"");
    System.out.println(s);
  }

}