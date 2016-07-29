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

package org.apache.fluo.recipes.spark;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.mapreduce.FluoEntryInputFormat;
import org.apache.fluo.mapreduce.FluoKeyValue;
import org.apache.fluo.mapreduce.FluoKeyValueGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Helper methods for using Fluo with Spark
 *
 * @since 1.0.0
 */
public class FluoSparkHelper {

  private static final Logger log = LoggerFactory.getLogger(FluoSparkHelper.class);
  private static AtomicInteger tempDirCounter = new AtomicInteger(0);
  private FluoConfiguration fluoConfig;
  private Configuration hadoopConfig;
  private Path tempBaseDir;
  private FileSystem hdfs;
  private Connector defaultConn;

  // @formatter:off
  public FluoSparkHelper(FluoConfiguration fluoConfig, Configuration hadoopConfig,
                         Path tempBaseDir) {
    // @formatter:on
    this.fluoConfig = fluoConfig;
    this.hadoopConfig = hadoopConfig;
    this.tempBaseDir = tempBaseDir;
    defaultConn = getAccumuloConnector(fluoConfig);
    try {
      hdfs = FileSystem.get(hadoopConfig);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to get HDFS client from hadoop config", e);
    }
  }

  /**
   * Converts RowColumnValue RDD to RowColumn/Bytes PairRDD
   *
   * @param rcvRDD RowColumnValue RDD to convert
   * @return RowColumn/Bytes PairRDD
   */
  public static JavaPairRDD<RowColumn, Bytes> toPairRDD(JavaRDD<RowColumnValue> rcvRDD) {
    return rcvRDD.mapToPair(rcv -> new Tuple2<>(rcv.getRowColumn(), rcv.getValue()));
  }

  /**
   * Converts RowColumn/Bytes PairRDD to RowColumnValue RDD
   *
   * @param pairRDD RowColumn/Bytes PairRDD
   * @return RowColumnValue RDD
   */
  public static JavaRDD<RowColumnValue> toRcvRDD(JavaPairRDD<RowColumn, Bytes> pairRDD) {
    return pairRDD.map(t -> new RowColumnValue(t._1().getRow(), t._1().getColumn(), t._2()));
  }

  private static Instance getInstance(FluoConfiguration config) {
    ClientConfiguration clientConfig =
        new ClientConfiguration().withInstance(config.getAccumuloInstance())
            .withZkHosts(config.getAccumuloZookeepers())
            .withZkTimeout(config.getZookeeperTimeout() / 1000);
    return new ZooKeeperInstance(clientConfig);
  }

  private static Connector getAccumuloConnector(FluoConfiguration config) {
    try {
      return getInstance(config).getConnector(config.getAccumuloUser(),
          new PasswordToken(config.getAccumuloPassword()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Reads all data in Fluo and returns it as a RowColumn/Value RDD
   *
   * @param ctx Java Spark context
   * @return RowColumn/Value RDD containing all data in Fluo
   */
  public JavaPairRDD<RowColumn, Bytes> readFromFluo(JavaSparkContext ctx) {
    Job job;
    try {
      job = Job.getInstance(hadoopConfig);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    FluoEntryInputFormat.configure(job, fluoConfig);

    return ctx.newAPIHadoopRDD(job.getConfiguration(), FluoEntryInputFormat.class, RowColumn.class,
        Bytes.class);
  }

  /**
   * Bulk import RowColumn/Value data into Fluo
   *
   * @param data RowColumn/Value data to import
   * @param opts Bulk import options
   */
  public void bulkImportRcvToFluo(JavaPairRDD<RowColumn, Bytes> data, BulkImportOptions opts) {

    data = partitionForAccumulo(data, fluoConfig.getAccumuloTable(), opts);

    JavaPairRDD<Key, Value> kvData = data.flatMapToPair(tuple -> {
      List<Tuple2<Key, Value>> output = new LinkedList<>();
      RowColumn rc = tuple._1();
      FluoKeyValueGenerator fkvg = new FluoKeyValueGenerator();
      fkvg.setRow(rc.getRow()).setColumn(rc.getColumn()).setValue(tuple._2().toArray());
      for (FluoKeyValue kv : fkvg.getKeyValues()) {
        output.add(new Tuple2<>(kv.getKey(), kv.getValue()));
      }
      return output;
    });

    bulkImportKvToAccumulo(kvData, fluoConfig.getAccumuloTable(), opts);
  }

  /**
   * Bulk import Key/Value data into Fluo
   *
   * @param data Key/Value data to import
   * @param opts Bulk import options
   */
  public void bulkImportKvToFluo(JavaPairRDD<Key, Value> data, BulkImportOptions opts) {
    bulkImportKvToAccumulo(data, fluoConfig.getAccumuloTable(), opts);
  }

  /**
   * Bulk import RowColumn/Value data into Accumulo
   *
   * @param data RowColumn/Value data to import
   * @param accumuloTable Accumulo table used for import
   * @param opts Bulk import options
   */
  public void bulkImportRcvToAccumulo(JavaPairRDD<RowColumn, Bytes> data, String accumuloTable,
      BulkImportOptions opts) {

    data = partitionForAccumulo(data, accumuloTable, opts);

    JavaPairRDD<Key, Value> kvData = data.mapToPair(tuple -> {
      RowColumn rc = tuple._1();
      byte[] row = rc.getRow().toArray();
      byte[] cf = rc.getColumn().getFamily().toArray();
      byte[] cq = rc.getColumn().getQualifier().toArray();
      byte[] val = tuple._2().toArray();
      return new Tuple2<>(new Key(new Text(row), new Text(cf), new Text(cq), 0), new Value(val));
    });
    bulkImportKvToAccumulo(kvData, accumuloTable, opts);
  }

  /**
   * Bulk import Key/Value data into Accumulo
   *
   * @param data Key/value data to import
   * @param accumuloTable Accumulo table used for import
   * @param opts Bulk import options
   */
  public void bulkImportKvToAccumulo(JavaPairRDD<Key, Value> data, String accumuloTable,
      BulkImportOptions opts) {

    Path tempDir = getTempDir(opts);
    Connector conn = chooseConnector(opts);

    try {
      if (hdfs.exists(tempDir)) {
        throw new IllegalArgumentException("HDFS temp dir already exists: " + tempDir.toString());
      }
      hdfs.mkdirs(tempDir);
      Path dataDir = new Path(tempDir.toString() + "/data");
      Path failDir = new Path(tempDir.toString() + "/fail");
      hdfs.mkdirs(failDir);

      // save data to HDFS
      Job job = Job.getInstance(hadoopConfig);
      AccumuloFileOutputFormat.setOutputPath(job, dataDir);
      // must use new API here as saveAsHadoopFile throws exception
      data.saveAsNewAPIHadoopFile(dataDir.toString(), Key.class, Value.class,
          AccumuloFileOutputFormat.class, job.getConfiguration());

      // bulk import data to Accumulo
      log.info("Wrote data for bulk import to HDFS temp directory: {}", dataDir);
      conn.tableOperations().importDirectory(accumuloTable, dataDir.toString(), failDir.toString(),
          false);

      // throw exception if failures directory contains files
      if (hdfs.listFiles(failDir, true).hasNext()) {
        throw new IllegalStateException("Bulk import failed!  Found files that failed to import "
            + "in failures directory: " + failDir);
      }
      log.info("Successfully bulk imported data in {} to '{}' Accumulo table", dataDir,
          accumuloTable);

      // delete data directory
      hdfs.delete(tempDir, true);
      log.info("Deleted HDFS temp directory created for bulk import: {}", tempDir);
      // @formatter:off
    } catch (IOException | TableNotFoundException | AccumuloException
        | AccumuloSecurityException e) {
      // @formatter:on
      throw new IllegalStateException(e);
    }
  }

  /**
   * Optional settings for Bulk Imports
   *
   * @since 1.0.0
   */
  public static class BulkImportOptions {

    public static BulkImportOptions DEFAULT = new BulkImportOptions();

    Connector conn = null;
    Path tempDir = null;

    public BulkImportOptions() {}

    public BulkImportOptions setAccumuloConnector(Connector conn) {
      Objects.requireNonNull(conn);
      this.conn = conn;
      return this;
    }

    public BulkImportOptions setTempDir(Path tempDir) {
      Objects.requireNonNull(tempDir);
      this.tempDir = tempDir;
      return this;
    }
  }

  private Path getPossibleTempDir() {
    return new Path(tempBaseDir.toString() + "/" + tempDirCounter.getAndIncrement());
  }

  private Path getTempDir(BulkImportOptions opts) {
    Path tempDir;
    if (opts.tempDir == null) {
      try {
        tempDir = getPossibleTempDir();
        while (hdfs.exists(tempDir)) {
          tempDir = getPossibleTempDir();
        }
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    } else {
      tempDir = opts.tempDir;
    }
    return tempDir;
  }

  private JavaPairRDD<RowColumn, Bytes> partitionForAccumulo(JavaPairRDD<RowColumn, Bytes> data,
      String accumuloTable, BulkImportOptions opts) {
    // partition and sort data so that one file is created per an accumulo tablet
    Partitioner accumuloPartitioner;
    try {
      accumuloPartitioner =
          new AccumuloRangePartitioner(chooseConnector(opts).tableOperations().listSplits(
              accumuloTable));
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      throw new IllegalStateException(e);
    }
    return data.repartitionAndSortWithinPartitions(accumuloPartitioner);
  }

  private Connector chooseConnector(BulkImportOptions opts) {
    return opts.conn == null ? defaultConn : opts.conn;
  }
}
