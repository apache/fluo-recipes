/*
 * Copyright 2016 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumnValue;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.mini.MiniFluo;
import io.fluo.recipes.accumulo.ops.TableOperations;
import io.fluo.recipes.common.Pirtos;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for creating integration tests that connect to a MiniFluo/MiniAccumuloCluster instance.
 */
public class FluoITHelper implements AutoCloseable {

  public static final String ACCUMULO_USER = "root";
  public static final String ACCUMULO_PASSWORD = "secret";
  private static final Logger log = LoggerFactory.getLogger(FluoITHelper.class);
  private Path baseDir;
  private FluoConfiguration fluoConfig;
  private MiniAccumuloCluster cluster;
  private MiniFluo miniFluo;
  private AtomicInteger fluoTableCounter = new AtomicInteger(1);
  private AtomicBoolean fluoRunning = new AtomicBoolean(false);

  public FluoITHelper(Path baseDir) {
    this.baseDir = baseDir;
    Objects.requireNonNull(baseDir);
    try {
      FileUtils.deleteDirectory(baseDir.toFile());
      MiniAccumuloConfig cfg = new MiniAccumuloConfig(baseDir.toFile(), ACCUMULO_PASSWORD);
      cluster = new MiniAccumuloCluster(cfg);
      cluster.start();
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException(e);
    }
    updateFluoConfig();
  }

  /**
   * Prints list of RowColumnValue objects
   *
   * @param rcvList RowColumnValue list
   */
  public static void printRowColumnValues(List<RowColumnValue> rcvList) {
    System.out.println("== RDD start ==");
    rcvList.forEach(rcv -> System.out.println("rc " + Hex.encNonAscii(rcv, " ")));
    System.out.println("== RDD end ==");
  }

  /**
   * Prints default Fluo table
   */
  public void printFluoTable() {
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      printFluoTable(fc);
    }
  }

  /**
   * Prints Fluo table accessible using provided client
   *
   * @param client Fluo client to table
   */
  public static void printFluoTable(FluoClient client) {
    try (Snapshot s = client.newSnapshot()) {
      RowIterator iter = s.get(new ScannerConfiguration());

      System.out.println("== fluo start ==");
      while (iter.hasNext()) {
        Map.Entry<Bytes, ColumnIterator> rowEntry = iter.next();
        ColumnIterator citer = rowEntry.getValue();
        while (citer.hasNext()) {
          Map.Entry<Column, Bytes> colEntry = citer.next();

          StringBuilder sb = new StringBuilder();
          Hex.encNonAscii(sb, rowEntry.getKey());
          sb.append(" ");
          Hex.encNonAscii(sb, colEntry.getKey(), " ");
          sb.append("\t");
          Hex.encNonAscii(sb, colEntry.getValue());

          System.out.println(sb.toString());
        }
      }
      System.out.println("=== fluo end ===");
    }
  }

  /**
   * Verifies that the actual data in the default Fluo instance of FluoITHelper matches expected
   * data
   *
   * @param expected RowColumnValue list containing expected data
   * @return True if actual data matches expected data
   */
  public boolean verifyFluoTable(List<RowColumnValue> expected) {
    try (FluoClient fc = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      return verifyFluoTable(fc, expected);
    }
  }

  /**
   * Verifies that the actual data in provided Fluo instance matches expected data
   *
   * @param client Fluo client to instance with actual data
   * @param expected RowColumnValue list containing expected data
   * @return True if actual data matches expected data
   */
  public static boolean verifyFluoTable(FluoClient client, List<RowColumnValue> expected) {
    try (Snapshot s = client.newSnapshot()) {
      RowIterator rowIter = s.get(new ScannerConfiguration());
      Iterator<RowColumnValue> rcvIter = expected.iterator();

      while (rowIter.hasNext()) {
        Map.Entry<Bytes, ColumnIterator> rowEntry = rowIter.next();
        ColumnIterator citer = rowEntry.getValue();
        while (citer.hasNext() && rcvIter.hasNext()) {
          Map.Entry<Column, Bytes> colEntry = citer.next();
          RowColumnValue rcv = rcvIter.next();
          Column col = colEntry.getKey();

          boolean retval = diff("fluo row", rcv.getRow(), rowEntry.getKey());
          retval |= diff("fluo fam", rcv.getColumn().getFamily(), col.getFamily());
          retval |= diff("fluo qual", rcv.getColumn().getQualifier(), col.getQualifier());
          retval |= diff("fluo val", rcv.getValue(), colEntry.getValue());

          if (retval) {
            log.error("Difference found - row {} cf {} cq {} val {}", rcv.getRow().toString(), rcv
                .getColumn().getFamily().toString(), rcv.getColumn().getQualifier().toString(), rcv
                .getValue().toString());
            return false;
          }

          log.debug("Verified {}", Hex.encNonAscii(rcv, " "));
        }
        if (citer.hasNext()) {
          log.error("An column iterator still has more data");
          return false;
        }
      }
      if (rowIter.hasNext() || rcvIter.hasNext()) {
        log.error("An iterator still has more data");
        return false;
      }
      log.debug("Actual data matched expected data");
      return true;
    }
  }

  /**
   * Prints specified Accumulo table in default Mini Accumulo instance
   *
   * @param accumuloTable Table to print
   */
  public void printAccumuloTable(String accumuloTable) {
    printAccumuloTable(getAccumuloConnector(), accumuloTable);
  }

  /**
   * Prints specified Accumulo table (accessible using Accumulo connector parameter)
   *
   * @param conn Accumulo connector of to instance with table to print
   * @param accumuloTable Accumulo table to print
   */
  public static void printAccumuloTable(Connector conn, String accumuloTable) {
    Scanner scanner = null;
    try {
      scanner = conn.createScanner(accumuloTable, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
    Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();

    System.out.println("== accumulo start ==");
    while (iterator.hasNext()) {
      Map.Entry<Key, Value> entry = iterator.next();
      System.out.println(entry.getKey() + " " + entry.getValue());
    }
    System.out.println("== accumulo end ==");
  }

  private static boolean diff(String dataType, String expected, String actual) {
    if (!expected.equals(actual)) {
      log.error("Difference found in {} - expected {} actual {}", dataType, expected, actual);
      return true;
    }
    return false;
  }

  private static boolean diff(String dataType, Bytes expected, Bytes actual) {
    if (!expected.equals(actual)) {
      log.error("Difference found in {} - expected {} actual {}", dataType,
          Hex.encNonAscii(expected), Hex.encNonAscii(actual));
      return true;
    }
    return false;
  }

  /**
   * Verifies that the actual data in Accumulo table matches expected data
   *
   * @param accumuloTable Accumulo table with actual data
   * @param expected RowColumnValue list containing expected data
   * @return True if actual data matches expected data
   */
  public boolean verifyAccumuloTable(String accumuloTable, List<RowColumnValue> expected) {
    return verifyAccumuloTable(getAccumuloConnector(), accumuloTable, expected);
  }

  /**
   * Verifies that actual data in Accumulo table matches expected data
   *
   * @param conn Connector to Accumulo instance with actual data
   * @param accumuloTable Accumulo table with actual data
   * @param expected RowColumnValue list containing expected data
   * @return True if actual data matches expected data
   */
  public static boolean verifyAccumuloTable(Connector conn, String accumuloTable,
      List<RowColumnValue> expected) {
    Scanner scanner;
    try {
      scanner = conn.createScanner(accumuloTable, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(e);
    }
    Iterator<Map.Entry<Key, Value>> scanIter = scanner.iterator();
    Iterator<RowColumnValue> rcvIter = expected.iterator();

    while (scanIter.hasNext() && rcvIter.hasNext()) {
      RowColumnValue rcv = rcvIter.next();
      Map.Entry<Key, Value> kvEntry = scanIter.next();
      Key key = kvEntry.getKey();
      Column col = rcv.getColumn();

      boolean retval = diff("row", rcv.getRow().toString(), key.getRow().toString());
      retval |= diff("fam", col.getFamily().toString(), key.getColumnFamily().toString());
      retval |= diff("qual", col.getQualifier().toString(), key.getColumnQualifier().toString());
      retval |= diff("val", rcv.getValue().toString(), kvEntry.getValue().toString());

      if (retval) {
        log.error("Difference found - row {} cf {} cq {} val {}", rcv.getRow().toString(), col
            .getFamily().toString(), col.getQualifier().toString(), rcv.getValue().toString());
        return false;
      }
      log.debug("Verified row {} cf {} cq {} val {}", rcv.getRow().toString(), col.getFamily()
          .toString(), col.getQualifier().toString(), rcv.getValue().toString());
    }

    if (scanIter.hasNext() || rcvIter.hasNext()) {
      log.error("An iterator still has more data");
      return false;
    }

    log.debug("Actual data matched expected data");
    return true;
  }

  /**
   * Verifies that expected list of RowColumnValues matches actual
   *
   * @param expected RowColumnValue list containing expected data
   * @param actual RowColumnValue list containing actual data
   * @return True if actual data matches expected data
   */
  public static boolean verifyRowColumnValues(List<RowColumnValue> expected,
      List<RowColumnValue> actual) {

    Iterator<RowColumnValue> expectIter = expected.iterator();
    Iterator<RowColumnValue> actualIter = actual.iterator();

    while (expectIter.hasNext() && actualIter.hasNext()) {
      RowColumnValue expRcv = expectIter.next();
      RowColumnValue actRcv = actualIter.next();

      boolean retval = diff("rcv row", expRcv.getRow(), actRcv.getRow());
      retval |= diff("rcv fam", expRcv.getColumn().getFamily(), actRcv.getColumn().getFamily());
      retval |=
          diff("rcv qual", expRcv.getColumn().getQualifier(), actRcv.getColumn().getQualifier());
      retval |= diff("rcv val", expRcv.getValue(), actRcv.getValue());

      if (retval) {
        log.error("Difference found in RowColumnValue lists - expected {} actual {}", expRcv,
            actRcv);
        return false;
      }
      log.debug("Verified row/col/val: {}", expRcv);
    }

    if (expectIter.hasNext() || actualIter.hasNext()) {
      log.error("A RowColumnValue list iterator still has more data");
      return false;
    }

    log.debug("Actual data matched expected data");
    return true;
  }

  /**
   * Retrieves MiniAccumuloCluster
   */
  public MiniAccumuloCluster getMiniAccumuloCluster() {
    return cluster;
  }

  /**
   * Retrieves MiniFluo
   */
  public synchronized MiniFluo getMiniFluo() {
    if (!fluoRunning.get()) {
      throw new IllegalStateException("Fluo is not running. The setupFluo() method must called "
          + "first");
    }
    return miniFluo;
  }

  /**
   * Returns an Accumulo Connector to MiniAccumuloCluster
   */
  public Connector getAccumuloConnector() {
    try {
      return cluster.getConnector(ACCUMULO_USER, ACCUMULO_PASSWORD);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Retrieves Fluo Configuration
   */
  public synchronized FluoConfiguration getFluoConfig() {
    return fluoConfig;
  }

  /**
   * Sets up Fluo by initializing Fluo instance and starting MiniFluo
   */
  public synchronized void setupFluo() {
    if (fluoRunning.get()) {
      throw new IllegalStateException("Attempting to setup Fluo when its already running");
    }

    try {
      FluoFactory.newAdmin(fluoConfig).initialize(
          new FluoAdmin.InitOpts().setClearTable(true).setClearZookeeper(true));
      TableOperations.optimizeTable(fluoConfig, Pirtos.getConfiguredOptimizations(fluoConfig));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    miniFluo = FluoFactory.newMiniFluo(fluoConfig);
    fluoRunning.set(true);
  }

  private void updateFluoConfig() {
    fluoConfig = new FluoConfiguration();
    fluoConfig.setMiniStartAccumulo(false);
    fluoConfig.setApplicationName("fluo-it");
    fluoConfig.setAccumuloInstance(cluster.getInstanceName());
    fluoConfig.setAccumuloUser(ACCUMULO_USER);
    fluoConfig.setAccumuloPassword(ACCUMULO_PASSWORD);
    fluoConfig.setInstanceZookeepers(cluster.getZooKeepers() + "/fluo");
    fluoConfig.setAccumuloZookeepers(cluster.getZooKeepers());
    fluoConfig.setAccumuloTable("fluo" + fluoTableCounter.getAndIncrement());
    fluoConfig.setWorkerThreads(5);
  }

  /**
   * Tears down Fluo by closing MiniFluo
   */
  public synchronized void teardownFluo() {
    if (!fluoRunning.get()) {
      throw new IllegalStateException("Attempting to teardown Fluo but it's not running");
    }
    try {
      miniFluo.close();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    updateFluoConfig();
    fluoRunning.set(false);
  }

  @Override
  public void close() {
    try {
      cluster.stop();
      FileUtils.deleteDirectory(baseDir.toFile());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
