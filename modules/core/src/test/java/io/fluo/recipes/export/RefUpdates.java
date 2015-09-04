/*
 * Copyright 2014 Fluo authors (see AUTHORS)
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

package io.fluo.recipes.export;

import java.util.HashSet;
import java.util.Set;

import io.fluo.recipes.serialization.SimpleSerializer;

class RefUpdates {
  private Set<String> addedRefs;
  private Set<String> deletedRefs;

  RefUpdates(Set<String> addedRefs, Set<String> deletedRefs) {
    this.addedRefs = addedRefs;
    this.deletedRefs = deletedRefs;
  }

  Set<String> getAddedRefs() {
    return addedRefs;
  }

  Set<String> getDeletedRefs() {
    return deletedRefs;
  }

  @Override
  public String toString() {
    return "added:" + addedRefs + " deleted:" + deletedRefs;
  }

  static SimpleSerializer<RefUpdates> newSerializer() {
    return new SimpleSerializer<RefUpdates>() {
      @Override
      public RefUpdates deserialize(byte[] b) {
        String[] rus = new String(b).split(",");

        Set<String> addedRefs = new HashSet<>();
        Set<String> deletedRefs = new HashSet<>();

        for (String update : rus) {
          if (update.startsWith("a")) {
            addedRefs.add(update.substring(1));
          } else if (update.startsWith("d")) {
            deletedRefs.add(update.substring(1));
          }
        }
        return new RefUpdates(addedRefs, deletedRefs);
      }

      @Override
      public byte[] serialize(RefUpdates e) {
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (String ar : e.getAddedRefs()) {
          sb.append(sep);
          sep = ",";
          sb.append("a");
          sb.append(ar);
        }

        for (String dr : e.getDeletedRefs()) {
          sb.append(sep);
          sep = ",";
          sb.append("d");
          sb.append(dr);
        }

        return sb.toString().getBytes();
      }
    };
  }
}
