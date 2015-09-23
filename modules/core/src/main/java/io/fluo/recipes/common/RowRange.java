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

package io.fluo.recipes.common;

import com.google.common.base.Preconditions;
import io.fluo.api.data.Bytes;

public class RowRange {
  private final Bytes start;
  private final Bytes end;

  public RowRange(Bytes start, Bytes end) {
    Preconditions.checkNotNull(start);
    Preconditions.checkNotNull(end);
    this.start = start;
    this.end = end;
  }

  public Bytes getStart() {
    return start;
  }

  public Bytes getEnd() {
    return end;
  }

  @Override
  public int hashCode() {
    return start.hashCode() + 31 * end.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RowRange) {
      RowRange or = (RowRange) o;
      return start.equals(or.start) && end.equals(or.end);
    }

    return false;
  }
}
