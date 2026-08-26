/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;


/**
 * Result of validating the segments assigned to one server for a table.
 * <p>
 * The server compares its current IdealState assignments with the locally loaded segments. ONLINE segments must be
 * loaded with the CRC recorded in ZK, while CONSUMING segments must be present. {@code maxEndTimeMs} is the maximum
 * end time among valid ONLINE segments assigned to that server, or {@code -1} when none are assigned.
 */
public class TableSegmentValidationInfo {
  private final boolean _valid;
  private final String _invalidReason;
  private final long _maxEndTimeMs;

  @JsonCreator
  public TableSegmentValidationInfo(@JsonProperty("valid") boolean valid,
      @JsonProperty("invalidReason") @Nullable String invalidReason,
      @JsonProperty("maxEndTimeMs") long maxEndTimeMs) {
    _valid = valid;
    _invalidReason = invalidReason;
    _maxEndTimeMs = maxEndTimeMs;
  }

  public boolean isValid() {
    return _valid;
  }

  public long getMaxEndTimeMs() {
    return _maxEndTimeMs;
  }

  @Nullable
  public String getInvalidReason() {
    return _invalidReason;
  }
}
