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

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.recipes.core.serialization.SimpleSerializer;

/**
 * @since 1.0.0
 * @deprecated since 1.1.0
 */
@Deprecated
public class ExportObserver<K, V> extends AbstractObserver {

  private ExportObserverImpl<K, V> eoi;

  private String queueId;

  protected String getQueueId() {
    return queueId;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Context context) throws Exception {

    queueId = context.getObserverConfiguration().getString("queueId");
    ExportQueue.Options opts = new ExportQueue.Options(queueId, context.getAppConfiguration());

    // TODO defer loading classes... so that not done during fluo init
    // TODO move class loading to centralized place... also attempt to check type params
    @SuppressWarnings("rawtypes")
    Exporter exporter =
        getClass().getClassLoader().loadClass(opts.fluentCfg.exporterType)
            .asSubclass(Exporter.class).newInstance();

    SimpleSerializer serializer = SimpleSerializer.getInstance(context.getAppConfiguration());

    exporter.init(new Exporter.Context() {

      @Override
      public String getQueueId() {
        return queueId;
      }

      @Override
      public SimpleConfiguration getExporterConfiguration() {
        return opts.getExporterConfiguration();
      }

      @Override
      public Context getObserverContext() {
        return context;
      }
    });


    this.eoi =
        new ExportObserverImpl<K, V>(queueId, opts.fluentCfg, serializer, exporter::processExports);

  }

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(ExportBucket.newNotificationColumn(queueId), NotificationType.WEAK);
  }

  @Override
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
    eoi.process(tx, row, col);
  }
}
