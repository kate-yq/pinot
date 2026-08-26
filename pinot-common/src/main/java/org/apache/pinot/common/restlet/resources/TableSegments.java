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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


/**
 * JSON wrapper for a list of segment names.
 * <p>
 * Used as the response from the server API that lists segments hosted for a table, and as the request body for APIs
 * that operate on a selected set of segments, such as valid-doc-ID metadata lookup.
 */
public class TableSegments {
  private final List<String> _segments;

  public TableSegments(@JsonProperty("segments") List<String> segments) {
    _segments = segments;
  }

  public List<String> getSegments() {
    return _segments;
  }
}
