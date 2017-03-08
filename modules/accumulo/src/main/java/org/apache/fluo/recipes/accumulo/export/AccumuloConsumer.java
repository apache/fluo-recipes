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
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.recipes.core.export.ExportConsumer;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.export.SequencedExport;

import org.apache.fluo.api.observer.ObserverProvider.Registry;

/**
 * An Accumulo-specific {@link ExportConsumer} that writes mutations to Accumulo. For an overview of
 * how to use this, see the project level documentation for exporting to Accumulo.
 * 
 * @see ExportQueue#registerObserver(Registry, ExportConsumer)
 * @since 1.1.0
 */
public class AccumuloConsumer<K, V> implements ExportConsumer<K, V> {

  private AccumuloTranslator<K, V> translator;
  private AccumuloWriter writer;

  /**
   * Use this to configure the Accumulo table where an AccumuloConsumer mutations will be written.
   *
   * @since 1.1.0
   */
  public static class Configuration {

    private final String instance;
    private final String zookeepers;
    private final String user;
    private final String password;
    private final String table;

    private static final String PREFIX = "recipes.accumulo.writer.";

    public Configuration(String instanceName, String zookeepers, String user, String password,
        String table) {
      this.instance = instanceName;
      this.zookeepers = zookeepers;
      this.user = user;
      this.password = password;
      this.table = table;
    }

    public String getInstance() {
      return instance;
    }

    public String getZookeepers() {
      return zookeepers;
    }

    public String getUser() {
      return user;
    }

    public String getPassword() {
      return password;
    }

    public String getTable() {
      return table;
    }

    public void save(String id, FluoConfiguration fluoConfig) {
      save(id, fluoConfig.getAppConfiguration());
    }

    public void save(String id, SimpleConfiguration appConfig) {
      appConfig.setProperty(PREFIX + id + ".instance", instance);
      appConfig.setProperty(PREFIX + id + ".zookeepers", zookeepers);
      appConfig.setProperty(PREFIX + id + ".user", user);
      appConfig.setProperty(PREFIX + id + ".password", password);
      appConfig.setProperty(PREFIX + id + ".table", table);
    }

    public static Configuration load(String id, SimpleConfiguration config) {
      String instance = config.getString(PREFIX + id + ".instance");
      String zookeepers = config.getString(PREFIX + id + ".zookeepers");
      String user = config.getString(PREFIX + id + ".user");
      String password = config.getString(PREFIX + id + ".password");
      String table = config.getString(PREFIX + id + ".table");

      return new Configuration(instance, zookeepers, user, password, table);
    }
  }

  public AccumuloConsumer(Configuration tableConfig, AccumuloTranslator<K, V> translator) {
    this.writer =
        AccumuloWriter.getInstance(tableConfig.getInstance(), tableConfig.getZookeepers(),
            tableConfig.user, tableConfig.getPassword(), tableConfig.getTable());
    this.translator = translator;
  }

  public AccumuloConsumer(String configId, SimpleConfiguration appConfig,
      AccumuloTranslator<K, V> translator) {
    this(Configuration.load(configId, appConfig), translator);
  }

  @Override
  public void accept(Iterator<SequencedExport<K, V>> t) {
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
