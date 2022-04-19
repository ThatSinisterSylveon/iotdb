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

package org.apache.iotdb.db.client.async;

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.commons.client.AsyncBaseClientFactory;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerProperty;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService;
import org.apache.iotdb.rpc.TNonblockingSocketWrapper;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

import java.io.IOException;

public class AsyncDataNodeDataBlockServiceClient extends DataBlockService.AsyncClient {

  private final EndPoint endpoint;
  private final ClientManager<AsyncDataNodeDataBlockServiceClient> clientManager;

  public AsyncDataNodeDataBlockServiceClient(
      TProtocolFactory protocolFactory,
      int connectionTimeout,
      EndPoint endpoint,
      TAsyncClientManager tClientManager,
      ClientManager<AsyncDataNodeDataBlockServiceClient> clientManager)
      throws IOException {
    super(
        protocolFactory,
        tClientManager,
        TNonblockingSocketWrapper.wrap(endpoint.getIp(), endpoint.getPort(), connectionTimeout));
    this.endpoint = endpoint;
    this.clientManager = clientManager;
  }

  public void close() {
    ___transport.close();
    ___currentMethod = null;
  }

  public boolean isValid() {
    return ___transport != null;
  }

  /**
   * return self if clientPool is not null, the method doesn't need to call by user, it will trigger
   * once client transport complete.
   */
  private void returnSelf() {
    if (clientManager != null) {
      clientManager.returnClient(endpoint, this);
    }
  }

  @Override
  public void onComplete() {
    super.onComplete();
    returnSelf();
  }

  public boolean isReady() {
    try {
      checkReady();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public static class Factory
      extends AsyncBaseClientFactory<EndPoint, AsyncDataNodeDataBlockServiceClient> {

    public Factory(
        ClientManager<AsyncDataNodeDataBlockServiceClient> clientManager,
        ClientManagerProperty<AsyncDataNodeDataBlockServiceClient> clientManagerProperty) {
      super(clientManager, clientManagerProperty);
    }

    @Override
    public void destroyObject(
        EndPoint endPoint, PooledObject<AsyncDataNodeDataBlockServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<AsyncDataNodeDataBlockServiceClient> makeObject(EndPoint endPoint)
        throws Exception {
      TAsyncClientManager tManager = tManagers[clientCnt.incrementAndGet() % tManagers.length];
      tManager = tManager == null ? new TAsyncClientManager() : tManager;
      return new DefaultPooledObject<>(
          new AsyncDataNodeDataBlockServiceClient(
              clientManagerProperty.getProtocolFactory(),
              clientManagerProperty.getConnectionTimeoutMs(),
              endPoint,
              tManager,
              clientManager));
    }

    @Override
    public boolean validateObject(
        EndPoint endPoint, PooledObject<AsyncDataNodeDataBlockServiceClient> pooledObject) {
      return pooledObject.getObject() != null && pooledObject.getObject().isValid();
    }
  }
}
