/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.accumulo.cmds;

import javax.inject.Inject;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.recipes.accumulo.ops.TableOperations;

public class CompactTransient {

  // when run with fluo exec command, the applications fluo config will be injected
  @Inject
  private static FluoConfiguration fluoConfig;

  public static void main(String[] args) throws Exception {

    if ((args.length == 1 && args[0].startsWith("-h")) || (args.length > 2)) {
      System.out
          .println("Usage : " + CompactTransient.class.getName() + " [<interval> [<count>]] ");

      System.exit(0);
    }

    int interval = 0;
    int count = 1;

    if (args.length >= 1) {
      interval = Integer.parseInt(args[0]);
      if (args.length == 2) {
        count = Integer.parseInt(args[1]);
      } else {
        count = Integer.MAX_VALUE;
      }
    }

    System.out.print("Compacting transient ranges ... ");
    TableOperations.compactTransient(fluoConfig);
    System.out.println("done.");
    count--;

    while (count > 0) {
      Thread.sleep(interval * 1000);
      System.out.print("Compacting transient ranges ... ");
      TableOperations.compactTransient(fluoConfig);
      System.out.println("done.");
      count--;
    }
  }
}
