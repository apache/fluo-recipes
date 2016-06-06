/*
 * Copyright 2016 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.recipes.accumulo.export;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Assert;
import org.junit.Test;

public class DifferenceExportTest {

  public class DiffExport extends DifferenceExport<String, String> {

    DiffExport(Optional<String> oldV, Optional<String> newV) {
      super(oldV, newV);
    }

    @Override
    protected Map<RowColumn, Bytes> generateData(String key, Optional<String> val) {
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
    Collection<Mutation> mutations;

    mutations = new DiffExport(Optional.empty(), Optional.of("a")).toMutations("k1", 1);
    Assert.assertEquals(1, mutations.size());
    Assert.assertTrue(mutations.contains(makePut("k1", "a", 1)));

    mutations = new DiffExport(Optional.of("ab"), Optional.of("ab")).toMutations("k2", 2);
    Assert.assertEquals(0, mutations.size());

    mutations = new DiffExport(Optional.of("b"), Optional.of("ab")).toMutations("k2", 2);
    Assert.assertEquals(1, mutations.size());
    Assert.assertTrue(mutations.contains(makePut("k2", "a", 2)));

    mutations = new DiffExport(Optional.of("c"), Optional.of("d")).toMutations("k3", 3);
    Assert.assertEquals(1, mutations.size());
    Mutation m = makeDel("k3", "c", 3);
    addPut(m, "k3", "d", 3);
    Assert.assertTrue(mutations.contains(m));

    mutations = new DiffExport(Optional.of("e"), Optional.empty()).toMutations("k4", 4);
    Assert.assertEquals(1, mutations.size());
    Assert.assertTrue(mutations.contains(makeDel("k4", "e", 4)));

    mutations = new DiffExport(Optional.of("ef"), Optional.of("fg")).toMutations("k5", 5);
    Assert.assertEquals(1, mutations.size());
    m = makeDel("k5", "e", 5);
    addPut(m, "k5", "g", 5);
    Assert.assertTrue(mutations.contains(m));
  }
}
