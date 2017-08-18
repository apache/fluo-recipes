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

package org.apache.fluo.recipes.core.map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.map.it.WordCountCombiner;
import org.junit.Assert;
import org.junit.Test;

@Deprecated
public class SplitsTest {
  private static List<Bytes> sort(List<Bytes> in) {
    ArrayList<Bytes> out = new ArrayList<>(in);
    Collections.sort(out);
    return out;
  }

  @Test
  public void testSplits() {

    org.apache.fluo.recipes.core.map.CollisionFreeMap.Options opts =
        new org.apache.fluo.recipes.core.map.CollisionFreeMap.Options("foo",
            WordCountCombiner.class, String.class, Long.class, 3);
    opts.setBucketsPerTablet(1);
    FluoConfiguration fluoConfig = new FluoConfiguration();
    CollisionFreeMap.configure(fluoConfig, opts);

    TableOptimizations tableOptim1 =
        new CollisionFreeMap.Optimizer().getTableOptimizations("foo",
            fluoConfig.getAppConfiguration());
    List<Bytes> expected1 =
        Lists.transform(
            Arrays.asList("foo:d:1", "foo:d:2", "foo:d:~", "foo:u:1", "foo:u:2", "foo:u:~"),
            Bytes::of);

    Assert.assertEquals(expected1, sort(tableOptim1.getSplits()));

    org.apache.fluo.recipes.core.map.CollisionFreeMap.Options opts2 =
        new org.apache.fluo.recipes.core.map.CollisionFreeMap.Options("bar",
            WordCountCombiner.class, String.class, Long.class, 6);
    opts2.setBucketsPerTablet(2);
    CollisionFreeMap.configure(fluoConfig, opts2);

    TableOptimizations tableOptim2 =
        new CollisionFreeMap.Optimizer().getTableOptimizations("bar",
            fluoConfig.getAppConfiguration());
    List<Bytes> expected2 =
        Lists.transform(
            Arrays.asList("bar:d:2", "bar:d:4", "bar:d:~", "bar:u:2", "bar:u:4", "bar:u:~"),
            Bytes::of);
    Assert.assertEquals(expected2, sort(tableOptim2.getSplits()));
  }
}
