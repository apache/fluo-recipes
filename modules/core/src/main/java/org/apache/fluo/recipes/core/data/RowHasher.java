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

package org.apache.fluo.recipes.core.data;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.recipes.core.common.TableOptimizations;
import org.apache.fluo.recipes.core.common.TableOptimizations.TableOptimizationsFactory;

/**
 * This recipe provides code to help add a hash of the row as a prefix of the row. Using this recipe
 * rows are structured like the following.
 * 
 * <p>
 * {@code <prefix>:<fixed len row hash>:<user row>}
 * 
 * <p>
 * The recipe also provides code the help generate split points and configure balancing of the
 * prefix.
 * 
 * <p>
 * The project documentation has more information.
 *
 * <p>
 * A single instance of RowHasher is thread safe. Creating a single static instance and using it can
 * result in good performance.
 *
 * @since 1.0.0
 */
public class RowHasher {

  private static final int HASH_LEN = 4;
  private static final String PREFIX = "recipes.rowHasher.";

  public static class Optimizer implements TableOptimizationsFactory {

    @Override
    public TableOptimizations getTableOptimizations(String key, SimpleConfiguration appConfig) {
      int numTablets = appConfig.getInt(PREFIX + key + ".numTablets");

      String prefix = key + ":";

      List<Bytes> splits = new ArrayList<>(numTablets - 1);

      int numSplits = numTablets - 1;
      int distance = (((int) Math.pow(Character.MAX_RADIX, HASH_LEN) - 1) / numTablets) + 1;
      int split = distance;
      for (int i = 0; i < numSplits; i++) {
        splits.add(Bytes.of(prefix
            + Strings.padStart(Integer.toString(split, Character.MAX_RADIX), HASH_LEN, '0')));
        split += distance;
      }

      splits.add(Bytes.of(prefix + "~"));


      TableOptimizations tableOptim = new TableOptimizations();
      tableOptim.setSplits(splits);
      tableOptim.setTabletGroupingRegex(Pattern.quote(prefix.toString()));

      return tableOptim;
    }

  }

  /**
   * This method can be called to register table optimizations before initializing Fluo. This will
   * register {@link Optimizer} with
   * {@link TableOptimizations#registerOptimization(SimpleConfiguration, String, Class)}. See the
   * project level documentation for an example.
   *
   * @param fluoConfig The config that will be used to initialize Fluo
   * @param prefix The prefix used for your Row Hasher. If you have a single instance, could call
   *        {@link RowHasher#getPrefix()}.
   * @param numTablets Initial number of tablet to create.
   */
  public static void configure(FluoConfiguration fluoConfig, String prefix, int numTablets) {
    fluoConfig.getAppConfiguration().setProperty(PREFIX + prefix + ".numTablets", numTablets);
    TableOptimizations.registerOptimization(fluoConfig.getAppConfiguration(), prefix,
        Optimizer.class);
  }

  private ThreadLocal<BytesBuilder> builders;
  private Bytes prefixBytes;
  private String prefix;

  public RowHasher(String prefix) {
    this.prefix = prefix;
    this.prefixBytes = Bytes.of(prefix + ":");

    builders = ThreadLocal.withInitial(() -> {
      BytesBuilder bb = Bytes.builder(prefixBytes.length() + 5 + 32);
      bb.append(prefixBytes);
      return bb;
    });
  }

  public String getPrefix() {
    return prefix;
  }

  /**
   * @return Returns input with prefix and hash of input prepended.
   */
  public Bytes addHash(String row) {
    return addHash(Bytes.of(row));
  }

  /**
   * @return Returns input with prefix and hash of input prepended.
   */
  public Bytes addHash(Bytes row) {
    BytesBuilder builder = builders.get();
    builder.setLength(prefixBytes.length());
    builder.append(genHash(row));
    builder.append(":");
    builder.append(row);
    return builder.toBytes();
  }

  private boolean hasHash(Bytes row) {
    for (int i = prefixBytes.length(); i < prefixBytes.length() + HASH_LEN; i++) {
      byte b = row.byteAt(i);
      boolean isAlphaNum = (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9');
      if (!isAlphaNum) {
        return false;
      }
    }

    if (row.byteAt(prefixBytes.length() - 1) != ':'
        || row.byteAt(prefixBytes.length() + HASH_LEN) != ':') {
      return false;
    }

    return true;
  }

  /**
   * @return Returns input with prefix and hash stripped from beginning.
   */
  public Bytes removeHash(Bytes row) {
    Preconditions.checkArgument(row.length() >= prefixBytes.length() + 5,
        "Row is shorter than expected " + row);
    Preconditions.checkArgument(row.subSequence(0, prefixBytes.length()).equals(prefixBytes),
        "Row does not have expected prefix " + row);
    Preconditions.checkArgument(hasHash(row), "Row does not have expected hash " + row);
    return row.subSequence(prefixBytes.length() + 5, row.length());
  }

  private static String genHash(Bytes row) {
    int hash = Hashing.murmur3_32().hashBytes(row.toArray()).asInt();
    hash = hash & 0x7fffffff;
    // base 36 gives a lot more bins in 4 bytes than hex, but it is still human readable which is
    // nice for debugging.
    String hashString =
        Strings.padStart(Integer.toString(hash, Character.MAX_RADIX), HASH_LEN, '0');
    hashString = hashString.substring(hashString.length() - HASH_LEN);
    return hashString;
  }
}
