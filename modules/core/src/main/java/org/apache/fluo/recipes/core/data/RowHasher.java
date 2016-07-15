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
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.BytesBuilder;
import org.apache.fluo.recipes.core.common.TableOptimizations;

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
 * @since 1.0.0
 */
public class RowHasher {

  private static final int HASH_LEN = 4;

  public TableOptimizations getTableOptimizations(int numTablets) {

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


  private Bytes prefix;

  public RowHasher(String prefix) {
    this.prefix = Bytes.of(prefix + ":");
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
    BytesBuilder builder = Bytes.newBuilder(prefix.length() + 5 + row.length());
    builder.append(prefix);
    builder.append(genHash(row));
    builder.append(":");
    builder.append(row);
    return builder.toBytes();
  }

  private boolean hasHash(Bytes row) {
    for (int i = prefix.length(); i < prefix.length() + HASH_LEN; i++) {
      byte b = row.byteAt(i);
      boolean isAlphaNum = (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9');
      if (!isAlphaNum) {
        return false;
      }
    }

    if (row.byteAt(prefix.length() - 1) != ':' || row.byteAt(prefix.length() + HASH_LEN) != ':') {
      return false;
    }

    return true;
  }

  /**
   * @return Returns input with prefix and hash stripped from beginning.
   */
  public Bytes removeHash(Bytes row) {
    Preconditions.checkArgument(row.length() >= prefix.length() + 5,
        "Row is shorter than expected " + row);
    Preconditions.checkArgument(row.subSequence(0, prefix.length()).equals(prefix),
        "Row does not have expected prefix " + row);
    Preconditions.checkArgument(hasHash(row), "Row does not have expected hash " + row);
    return row.subSequence(prefix.length() + 5, row.length());
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
