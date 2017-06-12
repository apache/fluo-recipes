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

package org.apache.fluo.recipes.accumulo.export;

import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloTranslator;
import org.apache.fluo.recipes.core.export.ExportQueue.Options;
import org.apache.fluo.recipes.core.export.Exporter;
import org.apache.fluo.recipes.core.export.SequencedExport;

/**
 * An Accumulo-specific {@link Exporter} that writes mutations to Accumulo. For an overview of how
 * to use this, see the project level documentation for exporting to Accumulo.
 *
 * @since 1.0.0
 * @deprecated since 1.1.0, replaced by
 *             {@link org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter} and
 *             {@link AccumuloTranslator}
 */
@Deprecated
public abstract class AccumuloExporter<K, V> extends Exporter<K, V> {

  /**
   * Use this to configure the Accumulo table where an AccumuloExporter's mutations will be written.
   * Create and pass to {@link Options#setExporterConfiguration(SimpleConfiguration)}
   *
   * @since 1.0.0
   */
  public static class Configuration extends SimpleConfiguration {

    private static final long serialVersionUID = 1L;

    public Configuration(String instanceName, String zookeepers, String user, String password,
        String table) {
      super.setProperty("instanceName", instanceName);
      super.setProperty("zookeepers", zookeepers);
      super.setProperty("user", user);
      super.setProperty("password", password);
      super.setProperty("table", table);
    }
  }

  private org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter<K, V> accumuloWriter;

  @Override
  public void init(Exporter.Context context) throws Exception {
    SimpleConfiguration sc = context.getExporterConfiguration();
    String instanceName = sc.getString("instanceName");
    String zookeepers = sc.getString("zookeepers");
    String user = sc.getString("user");
    String password = sc.getString("password");
    String table = sc.getString("table");

    FluoConfiguration tmpFc = new FluoConfiguration();
    org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter.configure("aecfgid")
        .instance(instanceName, zookeepers).credentials(user, password).table(table).save(tmpFc);
    accumuloWriter =
        new org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter<K, V>("aecfgid",
            tmpFc.getAppConfiguration(), this::translate);
  }

  @Override
  protected void processExports(Iterator<SequencedExport<K, V>> exports) {
    accumuloWriter.export(exports);
  }

  /**
   * Implementations of this method should translate the given SequencedExport to 0 or more
   * Mutations.
   *
   * @param export the input that should be translated to mutations
   * @param consumer output mutations to this consumer
   */
  protected abstract void translate(SequencedExport<K, V> export, Consumer<Mutation> consumer);

  /**
   * Generates Accumulo mutations by comparing the differences between a RowColumn/Bytes map that is
   * generated for old and new data and represents how the data should exist in Accumulo. When
   * comparing each row/column/value (RCV) of old and new data, mutations are generated using the
   * following rules:
   * <ul>
   * <li>If old and new data have the same RCV, nothing is done.
   * <li>If old and new data have same row/column but different values, an update mutation is
   * created for the row/column.
   * <li>If old data has a row/column that is not in the new data, a delete mutation is generated.
   * <li>If new data has a row/column that is not in the old data, an insert mutation is generated.
   * <li>Only one mutation is generated per row.
   * <li>The export sequence number is used for the timestamp in the mutation.
   * </ul>
   *
   * @param consumer generated mutations will be output to this consumer
   * @param oldData Map containing old row/column data
   * @param newData Map containing new row/column data
   * @param seq Export sequence number
   * @deprecated since 1.1.0 use
   *             {@link AccumuloTranslator#generateMutations(long, Map, Map, Consumer)}
   */
  @Deprecated
  public static void generateMutations(long seq, Map<RowColumn, Bytes> oldData,
      Map<RowColumn, Bytes> newData, Consumer<Mutation> consumer) {
    AccumuloTranslator.generateMutations(seq, oldData, newData, consumer);
  }
}
