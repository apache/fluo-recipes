/*
 * Copyright 2014 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.accumulo.export;

import java.util.ArrayList;

import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.configuration.Configuration;

import io.fluo.recipes.export.Exporter;
import io.fluo.recipes.serialization.SimpleSerializer;

public abstract class AccumuloExporter<K, V> extends Exporter<K, V> {

  private SharedBatchWriter sbw;
  private ArrayList<Mutation> buffer = new ArrayList<>();
  private long bufferSize = 0;

  protected AccumuloExporter(String queueId, Class<K> keyType, Class<V> valType,
      SimpleSerializer serializer) {
    super(queueId, keyType, valType, serializer);
  }

  @Override
  public void init(Context context) throws Exception {
    super.init(context);

    Configuration appConf = context.getAppConfiguration();

    String instanceName =
        appConf.getString("recipes.accumuloExporter." + getQueueId() + ".instance");
    String zookeepers =
        appConf.getString("recipes.accumuloExporter." + getQueueId() + ".zookeepers");
    String user = appConf.getString("recipes.accumuloExporter." + getQueueId() + ".user");
    // TODO look into using delegation token
    String password = appConf.getString("recipes.accumuloExporter." + getQueueId() + ".password");
    String table = appConf.getString("recipes.accumuloExporter." + getQueueId() + ".table");

    sbw = SharedBatchWriter.getInstance(instanceName, zookeepers, user, password, table);
  }

  public static void setExportTableInfo(Configuration appConf, String queueId, TableInfo ti) {
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".instance", ti.instanceName);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".zookeepers", ti.zookeepers);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".user", ti.user);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".password", ti.password);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".table", ti.table);
  }

  protected abstract Mutation convert(K key, long seq, V value);

  @Override
  protected void startingToProcessBatch() {
    buffer.clear();
    bufferSize = 0;
  }

  @Override
  public void processExport(K key, long sequenceNumber, V value) {
    Mutation m = convert(key, sequenceNumber, value);
    buffer.add(m);
    bufferSize += m.estimatedMemoryUsed();

    if (bufferSize > 1 << 20) {
      finishedProcessingBatch();
    }
  }

  @Override
  protected void finishedProcessingBatch() {
    if (buffer.size() > 0) {
      sbw.write(buffer);
      buffer.clear();
      bufferSize = 0;
    }
  }
}
