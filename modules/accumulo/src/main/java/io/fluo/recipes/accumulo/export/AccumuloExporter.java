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
import java.util.Collection;
import java.util.Iterator;

import io.fluo.api.observer.Observer.Context;
import io.fluo.recipes.export.Exporter;
import io.fluo.recipes.export.SequencedExport;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.configuration.Configuration;

public abstract class AccumuloExporter<K, V> extends Exporter<K, V> {

  private SharedBatchWriter sbw;

  @Override
  public void init(String queueId, Context context) throws Exception {

    Configuration appConf = context.getAppConfiguration();

    String instanceName = appConf.getString("recipes.accumuloExporter." + queueId + ".instance");
    String zookeepers = appConf.getString("recipes.accumuloExporter." + queueId + ".zookeepers");
    String user = appConf.getString("recipes.accumuloExporter." + queueId + ".user");
    // TODO look into using delegation token
    String password = appConf.getString("recipes.accumuloExporter." + queueId + ".password");
    String table = appConf.getString("recipes.accumuloExporter." + queueId + ".table");

    sbw = SharedBatchWriter.getInstance(instanceName, zookeepers, user, password, table);
  }

  public static void setExportTableInfo(Configuration appConf, String queueId, TableInfo ti) {
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".instance", ti.instanceName);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".zookeepers", ti.zookeepers);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".user", ti.user);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".password", ti.password);
    appConf.setProperty("recipes.accumuloExporter." + queueId + ".table", ti.table);
  }

  protected abstract Collection<Mutation> convert(K key, long seq, V value);

  @Override
  protected void processExports(Iterator<SequencedExport<K, V>> exports) {
    ArrayList<Mutation> buffer = new ArrayList<>();
    long bufferSize = 0;

    while (exports.hasNext()) {
      SequencedExport<K, V> export = exports.next();
      Collection<Mutation> mutationList =
          convert(export.getKey(), export.getSequence(), export.getValue());
      for (Mutation m : mutationList) {
        buffer.add(m);
        bufferSize += m.estimatedMemoryUsed();
      }
      if (bufferSize > 1 << 20) {
        sbw.write(buffer);
        buffer.clear();
        bufferSize = 0;
      }

    }
    if (buffer.size() > 0) {
      sbw.write(buffer);
    }
  }
}
