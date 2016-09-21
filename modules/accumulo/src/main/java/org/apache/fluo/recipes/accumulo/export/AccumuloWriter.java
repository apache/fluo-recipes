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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.api.config.SimpleConfiguration;

/**
 * Writes mutations to Accumulo using a shared batch writer
 *
 * @since 1.0.0
 */
class AccumuloWriter {

  private static class Mutations {
    List<Mutation> mutations;
    CountDownLatch cdl = new CountDownLatch(1);

    Mutations(Collection<Mutation> mutations) {
      this.mutations = new ArrayList<>(mutations);
    }
  }

  private static class ExportTask implements Runnable {

    private BatchWriter bw;

    ExportTask(String instanceName, String zookeepers, String user, String password, String table)
        throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
      ZooKeeperInstance zki =
          new ZooKeeperInstance(new ClientConfiguration().withInstance(instanceName).withZkHosts(
              zookeepers));

      // TODO need to close batch writer
      Connector conn = zki.getConnector(user, new PasswordToken(password));
      try {
        bw = conn.createBatchWriter(table, new BatchWriterConfig());
      } catch (TableNotFoundException tnfe) {
        try {
          conn.tableOperations().create(table);
        } catch (TableExistsException e) {
          // nothing to do
        }

        bw = conn.createBatchWriter(table, new BatchWriterConfig());
      }
    }

    @Override
    public void run() {

      ArrayList<AccumuloWriter.Mutations> exports = new ArrayList<>();

      while (true) {
        try {
          exports.clear();

          // gather export from all threads that have placed an item on the queue
          exports.add(exportQueue.take());
          exportQueue.drainTo(exports);

          for (AccumuloWriter.Mutations ml : exports) {
            bw.addMutations(ml.mutations);
          }

          bw.flush();

          // notify all threads waiting after flushing
          for (AccumuloWriter.Mutations ml : exports) {
            ml.cdl.countDown();
          }

        } catch (InterruptedException | MutationsRejectedException e) {
          throw new RuntimeException(e);
        }
      }
    }

  }

  private static LinkedBlockingQueue<AccumuloWriter.Mutations> exportQueue = null;

  private AccumuloWriter(String instanceName, String zookeepers, String user, String password,
      String table) {

    // TODO: fix this write to static and remove findbugs max rank override in pom.xml
    exportQueue = new LinkedBlockingQueue<>(10000);

    try {
      Thread queueProcessingTask =
          new Thread(new ExportTask(instanceName, zookeepers, user, password, table));
      queueProcessingTask.setDaemon(true);
      queueProcessingTask.start();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static Map<String, AccumuloWriter> exporters = new HashMap<>();


  static AccumuloWriter getInstance(SimpleConfiguration sc) {
    String instanceName = sc.getString("instanceName");
    String zookeepers = sc.getString("zookeepers");
    String user = sc.getString("user");
    String password = sc.getString("password");
    String table = sc.getString("table");
    return getInstance(instanceName, zookeepers, user, password, table);
  }

  static synchronized AccumuloWriter getInstance(String instanceName, String zookeepers,
      String user, String password, String table) {

    String key =
        instanceName + ":" + zookeepers + ":" + user + ":" + password.hashCode() + ":" + table;

    AccumuloWriter ret = exporters.get(key);

    if (ret == null) {
      ret = new AccumuloWriter(instanceName, zookeepers, user, password, table);
      exporters.put(key, ret);
    }

    return ret;
  }

  void write(Collection<Mutation> mutations) {
    AccumuloWriter.Mutations work = new Mutations(mutations);
    exportQueue.add(work);
    try {
      work.cdl.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}
