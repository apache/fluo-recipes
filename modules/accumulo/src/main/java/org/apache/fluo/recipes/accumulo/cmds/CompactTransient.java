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

package org.apache.fluo.recipes.accumulo.cmds;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.recipes.accumulo.ops.TableOperations;
import org.apache.fluo.recipes.core.common.RowRange;
import org.apache.fluo.recipes.core.common.TransientRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.0.0
 */
public class CompactTransient {

  // when run with fluo exec command, the applications fluo config will be injected
  @Inject
  private static FluoConfiguration fluoConfig;

  private static ScheduledExecutorService schedExecutor;

  private static Logger log = LoggerFactory.getLogger(CompactTransient.class);

  private static class CompactTask implements Runnable {

    private RowRange transientRange;
    private long requestedSleepTime;
    private double multiplier;

    public CompactTask(RowRange transientRange, long requestedSleepTime, double multiplier) {
      this.transientRange = transientRange;
      this.requestedSleepTime = requestedSleepTime;
      this.multiplier = multiplier;
    }

    @Override
    public void run() {

      long t1 = System.currentTimeMillis();

      try {
        TableOperations.compactTransient(fluoConfig, transientRange);
      } catch (Exception e) {
        log.warn("Compaction of " + transientRange + " failed ", e);
      }

      long t2 = System.currentTimeMillis();

      long sleepTime = Math.max((long) (multiplier * (t2 - t1)), requestedSleepTime);

      if (requestedSleepTime > 0) {
        log.info("Compacted {} in {}ms sleeping {}ms", transientRange, t2 - t1, sleepTime);
        schedExecutor.schedule(new CompactTask(transientRange, requestedSleepTime, multiplier),
            sleepTime, TimeUnit.MILLISECONDS);
      } else {
        log.info("Compacted {} in {}ms", transientRange, t2 - t1);
      }
    }
  }

  public static void main(String[] args) throws Exception {

    if ((args.length == 1 && args[0].startsWith("-h")) || (args.length > 2)) {
      System.out.println("Usage : " + CompactTransient.class.getName()
          + " [<interval> [<multiplier>]]");

      System.exit(-1);
    }

    int interval = 0;
    double multiplier = 3;

    if (args.length >= 1) {
      interval = Integer.parseInt(args[0]);
      if (args.length == 2) {
        multiplier = Double.parseDouble(args[1]);
      }
    }

    if (interval > 0) {
      schedExecutor = Executors.newScheduledThreadPool(1);
    }

    List<RowRange> transientRanges;

    try (FluoClient client = FluoFactory.newClient(fluoConfig)) {
      SimpleConfiguration appConfig = client.getAppConfiguration();

      TransientRegistry tr = new TransientRegistry(appConfig);

      transientRanges = tr.getTransientRanges();

      for (RowRange transientRange : transientRanges) {
        if (interval > 0) {
          schedExecutor.execute(new CompactTask(transientRange, interval * 1000, multiplier));
        } else {
          new CompactTask(transientRange, 0, 0).run();
        }
      }
    }

    if (interval > 0) {
      while (true) {
        Thread.sleep(10000);
      }
    }
  }
}
