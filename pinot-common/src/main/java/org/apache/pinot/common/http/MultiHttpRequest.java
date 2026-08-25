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
package org.apache.pinot.common.http;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;
import org.apache.pinot.spi.utils.retry.AttemptFailureException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to support multiple http operations in parallel by using the executor that is passed in. This is a wrapper
 * around Apache common HTTP client.
 */
public class MultiHttpRequest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiHttpRequest.class);

  private final Executor _executor;
  private final HttpClientConnectionManager _connectionManager;
  private final RetryPolicy _retryPolicy;

  /**
   * @param executor executor service to use for making parallel requests
   * @param connectionManager http connection manager to use.
   */
  public MultiHttpRequest(Executor executor, HttpClientConnectionManager connectionManager) {
    this(executor, connectionManager, RetryPolicies.noDelayRetryPolicy(1));
  }

  /**
   * @param executor executor service to use for making parallel requests
   * @param connectionManager http connection manager to use.
   * @param retryPolicy retry policy applied independently to each endpoint request. This retries only the failed
   *                    endpoints, unlike retrying the whole operation which would re-issue requests to endpoints that
   *                    already responded successfully. Use {@link RetryPolicies#noDelayRetryPolicy(int)} with a single
   *                    attempt to disable retries.
   */
  public MultiHttpRequest(Executor executor, HttpClientConnectionManager connectionManager, RetryPolicy retryPolicy) {
    _executor = executor;
    _connectionManager = connectionManager;
    _retryPolicy = retryPolicy;
  }

  /**
   * GET urls in parallel using the executor service.
   * @param urls absolute URLs to GET
   * @param requestHeaders headers to set when making the request
   * @param timeoutMs timeout in milliseconds for each GET request
   * @return instance of CompletionService. Completion service will provide
   *   results as they arrive. The order is NOT same as the order of URLs
   */
  public CompletionService<MultiHttpRequestResponse> executeGet(List<String> urls,
      @Nullable Map<String, String> requestHeaders, int timeoutMs) {
    List<Pair<String, String>> urlsAndRequestBodies = new ArrayList<>();
    urls.forEach(url -> urlsAndRequestBodies.add(Pair.of(url, "")));
    return execute(urlsAndRequestBodies, requestHeaders, timeoutMs, "GET", HttpGet::new);
  }

  /**
   * POST urls in parallel using the executor service.
   * @param urlsAndRequestBodies absolute URLs to POST
   * @param requestHeaders headers to set when making the request
   * @param timeoutMs timeout in milliseconds for each POST request
   * @return instance of CompletionService. Completion service will provide
   *   results as they arrive. The order is NOT same as the order of URLs
   */
  public CompletionService<MultiHttpRequestResponse> executePost(List<Pair<String, String>> urlsAndRequestBodies,
      @Nullable Map<String, String> requestHeaders, int timeoutMs) {
    return execute(urlsAndRequestBodies, requestHeaders, timeoutMs, "POST", HttpPost::new);
  }

  /**
   * Execute certain http method on the urls in parallel using the executor service.
   * @param urlsAndRequestBodies absolute URLs to execute the http method
   * @param requestHeaders headers to set when making the request
   * @param timeoutMs timeout in milliseconds for each http request
   * @param httpMethodName the name of the http method like GET, DELETE etc.
   * @param httpRequestBaseSupplier a function to create a new http method object.
   * @return instance of CompletionService. Completion service will provide
   *   results as they arrive. The order is NOT same as the order of URLs
   */
  public <T extends HttpUriRequestBase> CompletionService<MultiHttpRequestResponse> execute(
      List<Pair<String, String>> urlsAndRequestBodies, @Nullable Map<String, String> requestHeaders, int timeoutMs,
      String httpMethodName, Function<String, T> httpRequestBaseSupplier) {
    RequestConfig defaultRequestConfig = createRequestConfig(timeoutMs);

    HttpClientBuilder httpClientBuilder =
        HttpClients.custom().setConnectionManager(_connectionManager).setDefaultRequestConfig(defaultRequestConfig);

    CompletionService<MultiHttpRequestResponse> completionService = new ExecutorCompletionService<>(_executor);
    CloseableHttpClient client = httpClientBuilder.build();
    for (Pair<String, String> pair : urlsAndRequestBodies) {
      completionService.submit(() -> {
        String url = pair.getLeft();
        String body = pair.getRight();
        // The retry policy is applied per endpoint so that only this endpoint's request is retried on failure,
        // rather than re-issuing requests to endpoints that already responded successfully.
        AtomicReference<MultiHttpRequestResponse> responseHolder = new AtomicReference<>();
        AtomicReference<IOException> lastException = new AtomicReference<>();
        try {
          _retryPolicy.attempt(() -> {
            HttpUriRequestBase httpMethod = httpRequestBaseSupplier.apply(url);
            // If the http method is POST, set the request body
            if (httpMethod instanceof HttpPost) {
              ((HttpPost) httpMethod).setEntity(new StringEntity(body));
            }
            if (requestHeaders != null) {
              requestHeaders.forEach(httpMethod::setHeader);
            }
            CloseableHttpResponse response = null;
            try {
              response = client.execute(httpMethod);
              httpMethod.setAbsoluteRequestUri(true);
              responseHolder.set(new MultiHttpRequestResponse(URI.create(httpMethod.getRequestUri()), response));
              return true;
            } catch (IOException ex) {
              lastException.set(ex);
              if (response != null) {
                String error = EntityUtils.toString(response.getEntity());
                LOGGER.warn("Caught '{}' while executing: {} on URL: {}", error, httpMethodName, url);
              } else {
                // Log only exception type and message instead of the whole stack trace
                LOGGER.warn("Caught '{}' while executing: {} on URL: {}", ex, httpMethodName, url);
              }
              return false;
            }
          });
          return responseHolder.get();
        } catch (AttemptFailureException retryException) {
          // All retry attempts for this endpoint failed. Rethrow the original IO exception (if any) so callers can
          // reason about the failure (e.g. distinguish socket timeouts); otherwise surface the retry failure.
          IOException ioException = lastException.get();
          if (ioException != null) {
            throw ioException;
          }
          throw new IOException(
              "Failed to execute " + httpMethodName + " on URL: " + url + " after retries", retryException);
        }
      });
    }
    return completionService;
  }

  static RequestConfig createRequestConfig(int timeoutMs) {
    Timeout timeout = Timeout.of(timeoutMs, TimeUnit.MILLISECONDS);
    return RequestConfig.custom().setConnectionRequestTimeout(timeout).setConnectTimeout(timeout)
        .setResponseTimeout(timeout).build();
  }
}
