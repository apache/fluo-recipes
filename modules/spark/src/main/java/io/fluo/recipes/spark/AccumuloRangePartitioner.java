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

package io.fluo.recipes.spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.RowColumn;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partitioner;

public class AccumuloRangePartitioner extends Partitioner {

  private static final long serialVersionUID = 1L;
  private List<Bytes> splits;

  public AccumuloRangePartitioner(Collection<Text> listSplits) {
    this.splits = new ArrayList<>(listSplits.size());
    for (Text text : listSplits) {
      splits.add(Bytes.of(text.getBytes(), 0, text.getLength()));
    }
  }

  @Override
  public int getPartition(Object o) {
    RowColumn rc = (RowColumn) o;
    int index = Collections.binarySearch(splits, rc.getRow());
    index = index < 0 ? (index + 1) * -1 : index;
    return index;
  }

  @Override
  public int numPartitions() {
    return splits.size() + 1;
  }

}
