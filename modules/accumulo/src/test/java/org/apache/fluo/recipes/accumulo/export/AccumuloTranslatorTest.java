/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.recipes.accumulo.export;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloTranslator;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloTranslatorTest {

  public static Map<RowColumn, Bytes> genData(String key, Optional<String> val) {
    if (!val.isPresent()) {
      return Collections.emptyMap();
    }
    Map<RowColumn, Bytes> rcMap = new HashMap<>();
    String data = val.get();
    for (int i = 0; i < data.length(); i++) {
      char c = data.charAt(i);
      rcMap.put(new RowColumn("r:" + key, new Column("cf:" + c)), Bytes.of("v:" + c));
    }
    return rcMap;
  }

  public static void genMutations(String key, long seq, Optional<String> oldVal,
      Optional<String> newVal, Consumer<Mutation> consumer) {
    AccumuloTranslator.generateMutations(seq, genData(key, oldVal), genData(key, newVal), consumer);
  }

  public static Mutation makePut(String key, String val, long seq) {
    Mutation m = new Mutation("r:" + key);
    addPut(m, key, val, seq);
    return m;
  }

  public static void addPut(Mutation m, String key, String val, long seq) {
    m.put("cf:" + val, "", seq, "v:" + val);
  }

  public static Mutation makeDel(String key, String val, long seq) {
    Mutation m = new Mutation("r:" + key);
    addDel(m, key, val, seq);
    return m;
  }

  public static void addDel(Mutation m, String key, String val, long seq) {
    m.putDelete("cf:" + val, "", seq);
  }

  @Test
  public void testDifferenceExport() {
    final Collection<Mutation> mutations = new ArrayList<>();
    Consumer<Mutation> consumer = m -> mutations.add(m);

    genMutations("k1", 1, Optional.empty(), Optional.of("a"), consumer);
    Assert.assertEquals(1, mutations.size());
    Assert.assertTrue(mutations.contains(makePut("k1", "a", 1)));
    mutations.clear();

    genMutations("k2", 2, Optional.of("ab"), Optional.of("ab"), consumer);
    Assert.assertEquals(0, mutations.size());
    mutations.clear();

    genMutations("k2", 2, Optional.of("b"), Optional.of("ab"), consumer);
    Assert.assertEquals(1, mutations.size());
    Assert.assertTrue(mutations.contains(makePut("k2", "a", 2)));
    mutations.clear();

    genMutations("k3", 3, Optional.of("c"), Optional.of("d"), consumer);
    Assert.assertEquals(1, mutations.size());
    Mutation m = makeDel("k3", "c", 3);
    addPut(m, "k3", "d", 3);
    Assert.assertTrue(mutations.contains(m));
    mutations.clear();

    genMutations("k4", 4, Optional.of("e"), Optional.empty(), consumer);
    Assert.assertEquals(1, mutations.size());
    Assert.assertTrue(mutations.contains(makeDel("k4", "e", 4)));
    mutations.clear();

    genMutations("k5", 5, Optional.of("ef"), Optional.of("fg"), consumer);
    Assert.assertEquals(1, mutations.size());
    m = makeDel("k5", "e", 5);
    addPut(m, "k5", "g", 5);
    Assert.assertTrue(mutations.contains(m));
    mutations.clear();
  }
}
