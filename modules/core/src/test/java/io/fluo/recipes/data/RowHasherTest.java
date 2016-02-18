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

package io.fluo.recipes.data;

import io.fluo.api.data.Bytes;
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
    RowHasher rh = new RowHasher("p");
    String regex = rh.getTableOptimizations(3).getTabletGroupingRegex();
    Assert.assertEquals("(\\Qp:\\E).*", regex);
  }
}
