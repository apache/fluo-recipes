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

package org.apache.fluo.recipes.core.data;

import java.util.Arrays;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.junit.Assert;
import org.junit.Test;

public class RowHasherTest {

  @Test
  public void testBadPrefixes() {
    String[] badPrefixes =
        {"q:she6:test1", "q:she6:test1", "p:Mhe6:test1", "p;she6:test1", "p:she6;test1",
            "p;she6;test1", "p:+he6:test1", "p:s?e6:test1", "p:sh{6:test1", "p:sh6:"};

    RowHasher rh = new RowHasher("p");
    for (String badPrefix : badPrefixes) {
      try {
        rh.removeHash(Bytes.of(badPrefix));
        Assert.fail();
      } catch (IllegalArgumentException e) {
      }
    }
  }

  @Test
  public void testBasic() {
    RowHasher rh = new RowHasher("p");
    Assert.assertTrue(rh.removeHash(rh.addHash("abc")).toString().equals("abc"));
    rh = new RowHasher("p2");
    Assert.assertTrue(rh.removeHash(rh.addHash("abc")).toString().equals("abc"));

    Assert.assertTrue(rh.addHash("abc").toString().startsWith("p2:"));

    // test to ensure hash is stable over time
    Assert.assertEquals("p2:she6:test1", rh.addHash("test1").toString());
    Assert.assertEquals("p2:hgt0:0123456789abcdefghijklmnopqrstuvwxyz",
        rh.addHash("0123456789abcdefghijklmnopqrstuvwxyz").toString());
    Assert.assertEquals("p2:fluo:86ce3b094982c6a", rh.addHash("86ce3b094982c6a").toString());
  }

  @Test
  public void testBalancerRegex() {
    FluoConfiguration fc = new FluoConfiguration();
    RowHasher.configure(fc, "p", 3);
    TableOptimizations optimizations =
        new RowHasher.Optimizer().getTableOptimizations("p", fc.getAppConfiguration());
    String regex = optimizations.getTabletGroupingRegex();
    Assert.assertEquals("(\\Qp:\\E).*", regex);
    Assert.assertEquals(Arrays.asList(Bytes.of("p:c000"), Bytes.of("p:o000"), Bytes.of("p:~")),
        optimizations.getSplits());
  }
}
