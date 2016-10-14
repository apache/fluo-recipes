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

package org.apache.fluo.recipes.core.export;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.export.ExportQueue.Options;
import org.junit.Assert;
import org.junit.Test;

public class OptionsTest {
  @Test
  public void testExportQueueOptions() {
    FluoConfiguration conf = new FluoConfiguration();

    SimpleConfiguration ec1 = new SimpleConfiguration();
    ec1.setProperty("ep1", "ev1");
    ec1.setProperty("ep2", 3L);

    ExportQueue.configure(conf, new Options("Q1", "ET", "KT", "VT", 100));
    ExportQueue.configure(conf, new Options("Q2", "ET2", "KT2", "VT2", 200).setBucketsPerTablet(20)
        .setBufferSize(1000000).setExporterConfiguration(ec1));

    Options opts1 = new Options("Q1", conf.getAppConfiguration());

    Assert.assertEquals(opts1.exporterType, "ET");
    Assert.assertEquals(opts1.keyType, "KT");
    Assert.assertEquals(opts1.valueType, "VT");
    Assert.assertEquals(opts1.numBuckets, 100);
    Assert.assertEquals(opts1.bucketsPerTablet.intValue(), Options.DEFAULT_BUCKETS_PER_TABLET);
    Assert.assertEquals(opts1.bufferSize.intValue(), Options.DEFAULT_BUFFER_SIZE);

    Options opts2 = new Options("Q2", conf.getAppConfiguration());

    Assert.assertEquals(opts2.exporterType, "ET2");
    Assert.assertEquals(opts2.keyType, "KT2");
    Assert.assertEquals(opts2.valueType, "VT2");
    Assert.assertEquals(opts2.numBuckets, 200);
    Assert.assertEquals(opts2.bucketsPerTablet.intValue(), 20);
    Assert.assertEquals(opts2.bufferSize.intValue(), 1000000);

    SimpleConfiguration ec2 = opts2.getExporterConfiguration();

    Assert.assertEquals("ev1", ec2.getString("ep1"));
    Assert.assertEquals(3, ec2.getInt("ep2"));

  }
}
