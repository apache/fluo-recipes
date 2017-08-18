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

import java.util.List;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class OptionsTest {
  @Test
  @Deprecated
  public void testDeprecatedExportQueueOptions() {
    FluoConfiguration conf = new FluoConfiguration();

    SimpleConfiguration ec1 = new SimpleConfiguration();
    ec1.setProperty("ep1", "ev1");
    ec1.setProperty("ep2", 3L);

    ExportQueue.configure(conf, new org.apache.fluo.recipes.core.export.ExportQueue.Options("Q1",
        "ET", "KT", "VT", 100));
    ExportQueue.configure(conf, new org.apache.fluo.recipes.core.export.ExportQueue.Options("Q2",
        "ET2", "KT2", "VT2", 200).setBucketsPerTablet(20).setBufferSize(1000000)
        .setExporterConfiguration(ec1));

    org.apache.fluo.recipes.core.export.ExportQueue.Options opts1 =
        new org.apache.fluo.recipes.core.export.ExportQueue.Options("Q1",
            conf.getAppConfiguration());

    Assert.assertEquals(opts1.fluentCfg.exporterType, "ET");
    Assert.assertEquals(opts1.fluentCfg.keyType, "KT");
    Assert.assertEquals(opts1.fluentCfg.valueType, "VT");
    Assert.assertEquals(opts1.fluentCfg.buckets, 100);
    Assert.assertEquals(opts1.fluentCfg.bucketsPerTablet.intValue(),
        FluentConfigurator.DEFAULT_BUCKETS_PER_TABLET);
    Assert.assertEquals(opts1.fluentCfg.bufferSize.intValue(),
        FluentConfigurator.DEFAULT_BUFFER_SIZE);

    org.apache.fluo.recipes.core.export.ExportQueue.Options opts2 =
        new org.apache.fluo.recipes.core.export.ExportQueue.Options("Q2",
            conf.getAppConfiguration());

    Assert.assertEquals(opts2.fluentCfg.exporterType, "ET2");
    Assert.assertEquals(opts2.fluentCfg.keyType, "KT2");
    Assert.assertEquals(opts2.fluentCfg.valueType, "VT2");
    Assert.assertEquals(opts2.fluentCfg.buckets, 200);
    Assert.assertEquals(opts2.fluentCfg.bucketsPerTablet.intValue(), 20);
    Assert.assertEquals(opts2.fluentCfg.bufferSize.intValue(), 1000000);

    SimpleConfiguration ec2 = opts2.getExporterConfiguration();

    Assert.assertEquals("ev1", ec2.getString("ep1"));
    Assert.assertEquals(3, ec2.getInt("ep2"));

    List<org.apache.fluo.api.config.ObserverSpecification> obsSpecs =
        conf.getObserverSpecifications();
    Assert.assertTrue(obsSpecs.size() == 2);
    for (org.apache.fluo.api.config.ObserverSpecification ospec : obsSpecs) {
      Assert.assertEquals(ExportObserver.class.getName(), ospec.getClassName());
      String qid = ospec.getConfiguration().getString("queueId");
      Assert.assertTrue(qid.equals("Q1") || qid.equals("Q2"));
    }
  }

  @Test
  public void testExportQueueOptions() {
    FluoConfiguration conf = new FluoConfiguration();

    SimpleConfiguration ec1 = new SimpleConfiguration();
    ec1.setProperty("ep1", "ev1");
    ec1.setProperty("ep2", 3L);

    ExportQueue.configure("Q1").keyType("KT").valueType("VT").buckets(100).save(conf);
    ExportQueue.configure("Q2").keyType("KT2").valueType("VT2").buckets(200).bucketsPerTablet(20)
        .bufferSize(1000000).save(conf);

    FluentConfigurator opts1 = FluentConfigurator.load("Q1", conf.getAppConfiguration());

    Assert.assertNull(opts1.exporterType);
    Assert.assertEquals(opts1.keyType, "KT");
    Assert.assertEquals(opts1.valueType, "VT");
    Assert.assertEquals(opts1.buckets, 100);
    Assert.assertEquals(opts1.bucketsPerTablet.intValue(),
        FluentConfigurator.DEFAULT_BUCKETS_PER_TABLET);
    Assert.assertEquals(opts1.bufferSize.intValue(), FluentConfigurator.DEFAULT_BUFFER_SIZE);

    FluentConfigurator opts2 = FluentConfigurator.load("Q2", conf.getAppConfiguration());

    Assert.assertNull(opts2.exporterType);
    Assert.assertEquals(opts2.keyType, "KT2");
    Assert.assertEquals(opts2.valueType, "VT2");
    Assert.assertEquals(opts2.buckets, 200);
    Assert.assertEquals(opts2.bucketsPerTablet.intValue(), 20);
    Assert.assertEquals(opts2.bufferSize.intValue(), 1000000);
  }
}
