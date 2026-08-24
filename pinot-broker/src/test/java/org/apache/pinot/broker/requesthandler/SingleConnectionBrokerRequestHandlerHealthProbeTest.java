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
package org.apache.pinot.broker.requesthandler;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests the application-level HTTP readiness health probe used by the failure detector recovery path to avoid
 * re-admitting (and flapping) a half-dead server whose kernel still completes a TCP handshake.
 */
public class SingleConnectionBrokerRequestHandlerHealthProbeTest {
  private static final String INSTANCE_ID = "Server_localhost_1234";
  private static final int PROBE_TIMEOUT_MS = 2_000;

  private HttpServer _httpServer;
  private final AtomicInteger _responseStatus = new AtomicInteger(200);
  private String _adminEndpoint;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _httpServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    _httpServer.createContext("/health/readiness", exchange -> {
      int status = _responseStatus.get();
      byte[] body = (status == 200 ? "OK" : "Server is not ready").getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(status, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    _httpServer.start();
    _adminEndpoint = "http://localhost:" + _httpServer.getAddress().getPort();
  }

  @AfterMethod
  public void tearDown() {
    if (_httpServer != null) {
      _httpServer.stop(0);
    }
  }

  @Test
  public void testReadyWhenReadinessReturns200() {
    _responseStatus.set(200);
    assertTrue(
        SingleConnectionBrokerRequestHandler.isServerReadyViaHttp(_adminEndpoint, INSTANCE_ID, PROBE_TIMEOUT_MS));
  }

  @Test
  public void testNotReadyWhenReadinessReturns503() {
    // A half-dead server accepts the TCP connection but reports not-ready via readiness -> must NOT be re-admitted.
    _responseStatus.set(503);
    assertFalse(
        SingleConnectionBrokerRequestHandler.isServerReadyViaHttp(_adminEndpoint, INSTANCE_ID, PROBE_TIMEOUT_MS));
  }

  @Test
  public void testNotReadyWhenAdminEndpointUnreachable() {
    // No server listening on this port -> connection refused -> probe fails (server stays unhealthy).
    String deadEndpoint = "http://localhost:1";
    assertFalse(
        SingleConnectionBrokerRequestHandler.isServerReadyViaHttp(deadEndpoint, INSTANCE_ID, PROBE_TIMEOUT_MS));
  }
}
