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

package org.apache.fluo.recipes.core.combine;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class OptionsTest {
  @Test
  public void testExportQueueOptions() {
    FluoConfiguration conf = new FluoConfiguration();

    CombineQueue.configure("Q1").keyType("KT").valueType("VT").buckets(100).save(conf);
    CombineQueue.configure("Q2").keyType("KT2").valueType("VT2").buckets(200).bucketsPerTablet(20)
        .bufferSize(1000000).save(conf);

    SimpleConfiguration appConfig = conf.getAppConfiguration();

    Assert.assertEquals(CqConfigurator.getKeyType("Q1", appConfig), "KT");
    Assert.assertEquals(CqConfigurator.getValueType("Q1", appConfig), "VT");
    Assert.assertEquals(CqConfigurator.getNumBucket("Q1", appConfig), 100);
    Assert.assertEquals(CqConfigurator.getBucketsPerTablet("Q1", appConfig),
        CqConfigurator.DEFAULT_BUCKETS_PER_TABLET);
    Assert.assertEquals(CqConfigurator.getBufferSize("Q1", appConfig),
        CqConfigurator.DEFAULT_BUFFER_SIZE);

    Assert.assertEquals(CqConfigurator.getKeyType("Q2", appConfig), "KT2");
    Assert.assertEquals(CqConfigurator.getValueType("Q2", appConfig), "VT2");
    Assert.assertEquals(CqConfigurator.getNumBucket("Q2", appConfig), 200);
    Assert.assertEquals(CqConfigurator.getBucketsPerTablet("Q2", appConfig), 20);
    Assert.assertEquals(CqConfigurator.getBufferSize("Q2", appConfig), 1000000);
  }
}
