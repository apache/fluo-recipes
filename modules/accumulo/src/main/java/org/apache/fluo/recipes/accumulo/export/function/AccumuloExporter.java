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

package org.apache.fluo.recipes.accumulo.export.function;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.export.SequencedExport;
import org.apache.fluo.recipes.core.export.function.Exporter;

/**
 * An Accumulo-specific {@link Exporter} that writes mutations to Accumulo. For an overview of how
 * to use this, see the project level documentation for exporting to Accumulo.
 *
 * @see ExportQueue#registerObserver(ObserverProvider.Registry, Exporter)
 * @since 1.1.0
 */
public class AccumuloExporter<K, V> implements Exporter<K, V> {

  private AccumuloTranslator<K, V> translator;
  private AccumuloWriter writer;

  /**
   * Part of a fluent configuration API.
   *
   * @since 1.1.0
   */
  public static interface InstanceArgs {
    CredentialArgs instance(String instanceName, String zookeepers);
  }

  /**
   * Part of a fluent configuration API.
   *
   * @since 1.1.0
   */
  public static interface CredentialArgs {
    TableArgs credentials(String user, String password);
  }

  /**
   * Part of a fluent configuration API.
   *
   * @since 1.1.0
   */
  public static interface TableArgs {
    Options table(String tableName);
  }

  /**
   * Part of a fluent configuration API.
   *
   * @since 1.1.0
   */
  public static interface Options {
    void save(FluoConfiguration fluoConf);
  }

  public static InstanceArgs configure(String configId) {
    return new AeFluentConfigurator(configId);
  }

  public AccumuloExporter(String configId, SimpleConfiguration appConfig,
      AccumuloTranslator<K, V> translator) {
    AeFluentConfigurator cfg = AeFluentConfigurator.load(configId, appConfig);
    this.writer =
        AccumuloWriter.getInstance(cfg.getInstance(), cfg.getZookeepers(), cfg.getUser(),
            cfg.getPassword(), cfg.getTable());
    this.translator = translator;
  }

  @Override
  public void export(Iterator<SequencedExport<K, V>> t) {
    ArrayList<Mutation> buffer = new ArrayList<>();
    Consumer<Mutation> consumer = m -> buffer.add(m);

    while (t.hasNext()) {
      translator.translate(t.next(), consumer);
    }

    if (buffer.size() > 0) {
      writer.write(buffer);
    }
  }
}
