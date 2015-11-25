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

package io.fluo.recipes.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.fluo.api.data.Bytes;

public class BucketUtil {
  public static String genBucketId(int bucket, int maxBucket) {
    int bucketLen = Integer.toHexString(maxBucket).length();
    // TODO printf is slow
    return String.format("%0" + bucketLen + "x", bucket);
  }

  public static List<Bytes> shrink(List<Bytes> bl, int targetBucketsPerTablet) {

    if (targetBucketsPerTablet > bl.size()) {
      return Collections.emptyList();
    }

    // evenly spread the split points based on the target
    int bucketsPerTablet = bl.size() / (bl.size() / targetBucketsPerTablet);

    ArrayList<Bytes> ret = new ArrayList<>();
    for (int i = 0; i < bl.size(); i++) {
      if (i > 0 && i % bucketsPerTablet == 0 && (bl.size() - i) > bucketsPerTablet) {
        ret.add(bl.get(i));
      }
    }

    return ret;
  }
}
