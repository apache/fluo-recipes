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

package org.apache.fluo.recipes.core.common;

import java.util.Objects;

import org.apache.fluo.api.data.Bytes;

/**
 * @since 1.0.0
 */
public class RowRange {
  private final Bytes start;
  private final Bytes end;

  public RowRange(Bytes start, Bytes end) {
    Objects.requireNonNull(start);
    Objects.requireNonNull(end);
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

  private static void encNonAscii(StringBuilder sb, Bytes bytes) {
    if (bytes == null) {
      sb.append("null");
    } else {
      for (int i = 0; i < bytes.length(); i++) {
        byte b = bytes.byteAt(i);
        if (b >= 32 && b <= 126 && b != '\\') {
          sb.append((char) b);
        } else {
          sb.append(String.format("\\x%02x", b & 0xff));
        }
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    encNonAscii(sb, start);
    sb.append(", ");
    encNonAscii(sb, end);
    sb.append("]");
    return sb.toString();
  }
}
