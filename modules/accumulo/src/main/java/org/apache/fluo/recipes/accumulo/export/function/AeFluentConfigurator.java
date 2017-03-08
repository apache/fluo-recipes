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

import java.util.Objects;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter.CredentialArgs;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter.InstanceArgs;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter.Options;
import org.apache.fluo.recipes.accumulo.export.function.AccumuloExporter.TableArgs;

// Intentionally package private
class AeFluentConfigurator implements InstanceArgs, CredentialArgs, TableArgs, Options {

  private String id;
  private String instance;
  private String zookeepers;
  private String user;
  private String password;
  private String table;

  private static final String PREFIX = "recipes.accumulo.writer.";

  AeFluentConfigurator(String id) {
    this.id = id;
  }

  @Override
  public void save(FluoConfiguration fluoConf) {
    SimpleConfiguration appConfig = fluoConf.getAppConfiguration();
    // TODO Auto-generated method stub
    appConfig.setProperty(PREFIX + id + ".instance", instance);
    appConfig.setProperty(PREFIX + id + ".zookeepers", zookeepers);
    appConfig.setProperty(PREFIX + id + ".user", user);
    appConfig.setProperty(PREFIX + id + ".password", password);
    appConfig.setProperty(PREFIX + id + ".table", table);
  }

  @Override
  public Options table(String tableName) {
    this.table = Objects.requireNonNull(tableName);
    return this;
  }

  @Override
  public TableArgs credentials(String user, String password) {
    this.user = Objects.requireNonNull(user);
    this.password = Objects.requireNonNull(password);
    return this;
  }

  @Override
  public CredentialArgs instance(String instanceName, String zookeepers) {
    this.instance = Objects.requireNonNull(instanceName);
    this.zookeepers = Objects.requireNonNull(zookeepers);
    return this;
  }

  String getInstance() {
    return instance;
  }

  String getZookeepers() {
    return zookeepers;
  }

  String getUser() {
    return user;
  }

  String getPassword() {
    return password;
  }

  String getTable() {
    return table;
  }

  public static AeFluentConfigurator load(String id, SimpleConfiguration config) {
    AeFluentConfigurator aefc = new AeFluentConfigurator(id);
    aefc.instance = config.getString(PREFIX + id + ".instance");
    aefc.zookeepers = config.getString(PREFIX + id + ".zookeepers");
    aefc.user = config.getString(PREFIX + id + ".user");
    aefc.password = config.getString(PREFIX + id + ".password");
    aefc.table = config.getString(PREFIX + id + ".table");

    return aefc;
  }
}
