package org.tarantool.utils;

import static org.tarantool.handle.SQLParameterMapper.mapParameters;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SQLParameterMapperTest {

  @Test
  public void test() {
    Map<String, List<Integer>> stringListMap = mapParameters(
        "INSERT INTO security_type (id, bucket_id, version, name, type, cat_name) VALUES (?, ?, ?, ?, ?, ?)");
    System.out.println(stringListMap);
  }
}