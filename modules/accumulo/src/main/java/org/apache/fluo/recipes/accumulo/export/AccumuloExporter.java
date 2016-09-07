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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.observer.Observer.Context;
import org.apache.fluo.recipes.core.export.Exporter;
import org.apache.fluo.recipes.core.export.SequencedExport;

/**
 * An Accumulo-specific {@link Exporter} that writes mutations to Accumulo using a
 * {@link AccumuloExportQueue.AccumuloWriter}
 *
 * @since 1.0.0
 */
public abstract class AccumuloExporter<K, V> extends Exporter<K, V> {

  private AccumuloExportQueue.AccumuloWriter accumuloWriter;

  @Override
  public void init(String queueId, Context context) throws Exception {
    accumuloWriter =
        AccumuloExportQueue.AccumuloWriter.getInstance(context.getAppConfiguration(), queueId);
  }

  @Override
  protected void processExports(Iterator<SequencedExport<K, V>> exports) {

    ArrayList<Mutation> buffer = new ArrayList<>();

    while (exports.hasNext()) {
      SequencedExport<K, V> export = exports.next();
      buffer.addAll(processExport(export));
    }

    if (buffer.size() > 0) {
      accumuloWriter.write(buffer);
    }
  }

  protected abstract Collection<Mutation> processExport(SequencedExport<K, V> export);

}
