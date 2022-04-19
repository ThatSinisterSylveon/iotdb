/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.client;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import java.time.Duration;

public class ClientManagerProperty<T> {

  private final GenericKeyedObjectPoolConfig<T> config;

  // thrift client config
  private final TProtocolFactory protocolFactory;
  private int connectionTimeoutMs = 20_000;
  private int selectorNumOfAsyncClientPool = 1;

  public ClientManagerProperty(
      GenericKeyedObjectPoolConfig<T> config,
      TProtocolFactory protocolFactory,
      int connectionTimeoutMs,
      int selectorNumOfAsyncClientPool) {
    this.config = config;
    this.protocolFactory = protocolFactory;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.selectorNumOfAsyncClientPool = selectorNumOfAsyncClientPool;
  }

  public GenericKeyedObjectPoolConfig<T> getConfig() {
    return config;
  }

  public TProtocolFactory getProtocolFactory() {
    return protocolFactory;
  }

  public int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  public int getSelectorNumOfAsyncClientPool() {
    return selectorNumOfAsyncClientPool;
  }

  public static class Builder<T> {

    // pool config
    private long waitClientTimeoutMS = 20_000;
    private int maxConnectionForEachNode = 1_000;
    private int maxIdleConnectionForEachNode = 1_000;

    // thrift client config
    private boolean rpcThriftCompressionEnabled = false;
    private int connectionTimeoutMs = 20_000;
    private int selectorNumOfAsyncClientPool = 1;

    public Builder<T> setWaitClientTimeoutMS(long waitClientTimeoutMS) {
      this.waitClientTimeoutMS = waitClientTimeoutMS;
      return this;
    }

    public Builder<T> setMaxConnectionForEachNode(int maxConnectionForEachNode) {
      this.maxConnectionForEachNode = maxConnectionForEachNode;
      return this;
    }

    public Builder<T> setMaxIdleConnectionForEachNode(int maxIdleConnectionForEachNode) {
      this.maxIdleConnectionForEachNode = maxIdleConnectionForEachNode;
      return this;
    }

    public Builder<T> setRpcThriftCompressionEnabled(boolean rpcThriftCompressionEnabled) {
      this.rpcThriftCompressionEnabled = rpcThriftCompressionEnabled;
      return this;
    }

    public Builder<T> setConnectionTimeoutMs(int connectionTimeoutMs) {
      this.connectionTimeoutMs = connectionTimeoutMs;
      return this;
    }

    public Builder<T> setSelectorNumOfAsyncClientPool(int selectorNumOfAsyncClientPool) {
      this.selectorNumOfAsyncClientPool = selectorNumOfAsyncClientPool;
      return this;
    }

    public ClientManagerProperty<T> build() {
      GenericKeyedObjectPoolConfig<T> poolConfig = new GenericKeyedObjectPoolConfig<>();
      poolConfig.setMaxTotalPerKey(maxConnectionForEachNode);
      poolConfig.setMaxIdlePerKey(maxIdleConnectionForEachNode);
      poolConfig.setMaxWait(Duration.ofMillis(waitClientTimeoutMS));
      poolConfig.setTestOnReturn(true);
      poolConfig.setTestOnBorrow(true);
      return new ClientManagerProperty<>(
          poolConfig,
          rpcThriftCompressionEnabled
              ? new TCompactProtocol.Factory()
              : new TBinaryProtocol.Factory(),
          connectionTimeoutMs,
          selectorNumOfAsyncClientPool);
    }
  }
}
