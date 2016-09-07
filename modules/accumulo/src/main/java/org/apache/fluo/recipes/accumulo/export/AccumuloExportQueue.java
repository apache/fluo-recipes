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
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.recipes.core.export.ExportQueue;

public class AccumuloExportQueue {

  /**
   * Configures AccumuloExportQueue
   *
   * @param fc Fluo configuration
   * @param eqo Export queue options
   * @param ao Accumulo export queue options
   */
  public static void configure(FluoConfiguration fc, ExportQueue.Options eqo, Options ao) {
    ExportQueue.configure(fc, eqo);
    AccumuloWriter.setConfig(fc.getAppConfiguration(), eqo.getQueueId(), ao);
  }

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
   * @param oldData Map containing old row/column data
   * @param newData Map containing new row/column data
   * @param seq Export sequence number
   */
  public static Collection<Mutation> generateMutations(long seq, Map<RowColumn, Bytes> oldData,
      Map<RowColumn, Bytes> newData) {
    Map<Bytes, Mutation> mutationMap = new HashMap<>();
    for (Map.Entry<RowColumn, Bytes> entry : oldData.entrySet()) {
      RowColumn rc = entry.getKey();
      if (!newData.containsKey(rc)) {
        Mutation m = mutationMap.computeIfAbsent(rc.getRow(), r -> new Mutation(r.toArray()));
        m.putDelete(rc.getColumn().getFamily().toArray(), rc.getColumn().getQualifier().toArray(),
            seq);
      }
    }
    for (Map.Entry<RowColumn, Bytes> entry : newData.entrySet()) {
      RowColumn rc = entry.getKey();
      Column col = rc.getColumn();
      Bytes newVal = entry.getValue();
      Bytes oldVal = oldData.get(rc);
      if (oldVal == null || !oldVal.equals(newVal)) {
        Mutation m = mutationMap.computeIfAbsent(rc.getRow(), r -> new Mutation(r.toArray()));
        m.put(col.getFamily().toArray(), col.getQualifier().toArray(), seq, newVal.toArray());
      }
    }
    return mutationMap.values();
  }

  /**
   * Writes mutations to Accumulo using a shared batch writer
   *
   * @since 1.0.0
   */
  static class AccumuloWriter {

    private static class Mutations {
      List<Mutation> mutations;
      CountDownLatch cdl = new CountDownLatch(1);

      Mutations(Collection<Mutation> mutations) {
        this.mutations = new ArrayList<>(mutations);
      }
    }

    /**
     * Sets AccumuloWriter config in app configuration
     */
    static void setConfig(SimpleConfiguration sc, String id, Options ac) {
      String prefix = "recipes.accumulo.writer." + id;
      sc.setProperty(prefix + ".instance", ac.instanceName);
      sc.setProperty(prefix + ".zookeepers", ac.zookeepers);
      sc.setProperty(prefix + ".user", ac.user);
      sc.setProperty(prefix + ".password", ac.password);
      sc.setProperty(prefix + ".table", ac.table);
    }

    /**
     * Gets Accumulo Options from app configuration
     */
    static Options getConfig(SimpleConfiguration sc, String id) {
      String prefix = "recipes.accumulo.writer." + id;
      String instanceName = sc.getString(prefix + ".instance");
      String zookeepers = sc.getString(prefix + ".zookeepers");
      String user = sc.getString(prefix + ".user");
      String password = sc.getString(prefix + ".password");
      String table = sc.getString(prefix + ".table");
      return new Options(instanceName, zookeepers, user, password, table);
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

        ArrayList<Mutations> exports = new ArrayList<>();

        while (true) {
          try {
            exports.clear();

            // gather export from all threads that have placed an item on the queue
            exports.add(exportQueue.take());
            exportQueue.drainTo(exports);

            for (Mutations ml : exports) {
              bw.addMutations(ml.mutations);
            }

            bw.flush();

            // notify all threads waiting after flushing
            for (Mutations ml : exports) {
              ml.cdl.countDown();
            }

          } catch (InterruptedException | MutationsRejectedException e) {
            throw new RuntimeException(e);
          }
        }
      }

    }

    private static LinkedBlockingQueue<Mutations> exportQueue = null;

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


    static AccumuloWriter getInstance(SimpleConfiguration sc, String id) {
      return getInstance(getConfig(sc, id));
    }

    static AccumuloWriter getInstance(Options ac) {
      return getInstance(ac.instanceName, ac.zookeepers, ac.user, ac.password, ac.table);
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
      Mutations work = new Mutations(mutations);
      exportQueue.add(work);
      try {
        work.cdl.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

  }

  /**
   * Accumulo export queue options
   *
   * @since 1.0.0
   */
  public static class Options {
    String instanceName;
    String zookeepers;
    String user;
    String password;
    String table;

    public Options(String instanceName, String zookeepers, String user, String password,
        String table) {
      this.instanceName = instanceName;
      this.zookeepers = zookeepers;
      this.user = user;
      this.password = password;
      this.table = table;
    }
  }
}
