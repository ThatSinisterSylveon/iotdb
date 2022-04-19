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

package org.apache.iotdb.db.client;

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerProperty;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.client.async.AsyncConfigNodeIServiceClient;
import org.apache.iotdb.db.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class ClientPoolFactory {

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  public static class SyncConfigNodeIServiceClientPoolFactory
      implements IClientPoolFactory<EndPoint, SyncConfigNodeIServiceClient> {
    @Override
    public KeyedObjectPool<EndPoint, SyncConfigNodeIServiceClient> createClientPool(
        ClientManager<EndPoint, SyncConfigNodeIServiceClient> manager) {
      ClientManagerProperty<SyncConfigNodeIServiceClient> property =
          new ClientManagerProperty.Builder<SyncConfigNodeIServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .build();
      return new GenericKeyedObjectPool<>(
          new SyncConfigNodeIServiceClient.Factory(manager, property), property.getConfig());
    }
  }

  public static class AsyncConfigNodeIServiceClientPoolFactory
      implements IClientPoolFactory<EndPoint, AsyncConfigNodeIServiceClient> {
    @Override
    public KeyedObjectPool<EndPoint, AsyncConfigNodeIServiceClient> createClientPool(
        ClientManager<EndPoint, AsyncConfigNodeIServiceClient> manager) {
      ClientManagerProperty<AsyncConfigNodeIServiceClient> property =
          new ClientManagerProperty.Builder<AsyncConfigNodeIServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .setSelectorNumOfAsyncClientPool(conf.getSelectorNumOfClientPool())
              .build();
      return new GenericKeyedObjectPool<>(
          new AsyncConfigNodeIServiceClient.Factory(manager, property), property.getConfig());
    }
  }

  public static class SyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<EndPoint, SyncDataNodeInternalServiceClient> {
    @Override
    public KeyedObjectPool<EndPoint, SyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<EndPoint, SyncDataNodeInternalServiceClient> manager) {
      ClientManagerProperty<SyncDataNodeInternalServiceClient> property =
          new ClientManagerProperty.Builder<SyncDataNodeInternalServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .build();
      return new GenericKeyedObjectPool<>(
          new SyncDataNodeInternalServiceClient.Factory(manager, property), property.getConfig());
    }
  }

  public static class AsyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<EndPoint, AsyncDataNodeInternalServiceClient> {
    @Override
    public KeyedObjectPool<EndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<EndPoint, AsyncDataNodeInternalServiceClient> manager) {
      ClientManagerProperty<AsyncDataNodeInternalServiceClient> property =
          new ClientManagerProperty.Builder<AsyncDataNodeInternalServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .setSelectorNumOfAsyncClientPool(conf.getSelectorNumOfClientPool())
              .build();
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeInternalServiceClient.Factory(manager, property), property.getConfig());
    }
  }
}
