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

package org.apache.fluo.recipes.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper for creating integration tests that connect to a MiniFluo/MiniAccumuloCluster instance.
 *
 * @since 1.0.0
 */
public class FluoITHelper {

  private static final Logger log = LoggerFactory.getLogger(FluoITHelper.class);

  /**
   * Prints list of RowColumnValue objects
   *
   * @param rcvList RowColumnValue list
   */
  public static void printRowColumnValues(Collection<RowColumnValue> rcvList) {
    System.out.println("== RDD start ==");
    rcvList.forEach(rcv -> System.out.println("rc " + Hex.encNonAscii(rcv, " ")));
    System.out.println("== RDD end ==");
  }

  public static void printFluoTable(FluoConfiguration conf) {
    try (FluoClient client = FluoFactory.newClient(conf)) {
      printFluoTable(client);
    }
  }

  /**
   * Prints Fluo table accessible using provided client
   *
   * @param client Fluo client to table
   */
  public static void printFluoTable(FluoClient client) {
    try (Snapshot s = client.newSnapshot()) {
      System.out.println("== fluo start ==");
      for (RowColumnValue rcv : s.scanner().build()) {
        StringBuilder sb = new StringBuilder();
        Hex.encNonAscii(sb, rcv.getRow());
        sb.append(" ");
        Hex.encNonAscii(sb, rcv.getColumn(), " ");
        sb.append("\t");
        Hex.encNonAscii(sb, rcv.getValue());

        System.out.println(sb.toString());
      }
      System.out.println("=== fluo end ===");
    }
  }

  // @formatter:off
  public static boolean verifyFluoTable(FluoConfiguration conf,
                                        Collection<RowColumnValue> expected) {
    // @formatter:on
    try (FluoClient client = FluoFactory.newClient(conf)) {
      return verifyFluoTable(client, expected);
    }
  }

  /**
   * Verifies that the actual data in provided Fluo instance matches expected data
   *
   * @param client Fluo client to instance with actual data
   * @param expected RowColumnValue list containing expected data
   * @return True if actual data matches expected data
   */
  public static boolean verifyFluoTable(FluoClient client, Collection<RowColumnValue> expected) {

    expected = sort(expected);

    try (Snapshot s = client.newSnapshot()) {
      Iterator<RowColumnValue> fluoIter = s.scanner().build().iterator();
      Iterator<RowColumnValue> rcvIter = expected.iterator();

      while (fluoIter.hasNext() && rcvIter.hasNext()) {
        RowColumnValue actualRcv = fluoIter.next();
        RowColumnValue rcv = rcvIter.next();

        boolean retval = diff("fluo row", rcv.getRow(), actualRcv.getRow());
        retval |= diff("fluo fam", rcv.getColumn().getFamily(), actualRcv.getColumn().getFamily());
        retval |=
            diff("fluo qual", rcv.getColumn().getQualifier(), actualRcv.getColumn().getQualifier());
        retval |= diff("fluo val", rcv.getValue(), actualRcv.getValue());

        if (retval) {
          log.error("Difference found - row {} cf {} cq {} val {}", rcv.getsRow(), rcv.getColumn()
              .getsFamily(), rcv.getColumn().getsQualifier(), rcv.getsValue());
          return false;
        }

        log.debug("Verified {}", Hex.encNonAscii(rcv, " "));
      }

      if (fluoIter.hasNext() || rcvIter.hasNext()) {
        log.error("An iterator still has more data");
        return false;
      }
      log.debug("Actual data matched expected data");
      return true;
    }
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
   * Verifies that actual data in Accumulo table matches expected data
   *
   * @param conn Connector to Accumulo instance with actual data
   * @param accumuloTable Accumulo table with actual data
   * @param expected RowColumnValue list containing expected data
   * @return True if actual data matches expected data
   */
  public static boolean verifyAccumuloTable(Connector conn, String accumuloTable,
      Collection<RowColumnValue> expected) {

    expected = sort(expected);

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
  public static boolean verifyRowColumnValues(Collection<RowColumnValue> expected,
      Collection<RowColumnValue> actual) {

    expected = sort(expected);
    actual = sort(actual);

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

  private static List<RowColumnValue> sort(Collection<RowColumnValue> input) {
    ArrayList<RowColumnValue> copy = new ArrayList<>(input);
    Collections.sort(copy);
    return copy;
  }

  /**
   * A helper method for parsing test data. Each string passed in is expected to have the following
   * format {@literal <row>|<family>|<qualifier>|<value>}
   */
  public static List<RowColumnValue> parse(String... data) {
    return parse(Splitter.on('|'), data);
  }

  /**
   * A helper method for parsing test data. Each string passed in is split using the specified
   * splitter into four fields for row, family, qualifier, and value.
   */
  public static List<RowColumnValue> parse(Splitter splitter, String... data) {
    ArrayList<RowColumnValue> ret = new ArrayList<>();

    for (String line : data) {
      Iterable<String> cols = splitter.split(line);
      if (Iterables.size(cols) != 4) {
        throw new IllegalArgumentException("Bad input " + line);
      }

      Iterator<String> iter = cols.iterator();
      RowColumnValue rcv =
          new RowColumnValue(Bytes.of(iter.next()), new Column(iter.next(), iter.next()),
              Bytes.of(iter.next()));

      ret.add(rcv);
    }

    return ret;
  }
}
