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

package org.apache.iotdb.db.client.sync;

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.commons.client.BaseClientFactory;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerProperty;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TimeoutChangeableTransport;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketException;

public class SyncConfigNodeIServiceClient extends ConfigIService.Client {

  private final EndPoint endpoint;
  private final ClientManager<EndPoint, SyncConfigNodeIServiceClient> clientManager;

  public SyncConfigNodeIServiceClient(
      TProtocolFactory protocolFactory,
      int connectionTimeout,
      EndPoint endpoint,
      ClientManager<EndPoint, SyncConfigNodeIServiceClient> clientManager)
      throws TTransportException {
    super(
        protocolFactory.getProtocol(
            RpcTransportFactory.INSTANCE.getTransport(
                new TSocket(
                    TConfigurationConst.defaultTConfiguration,
                    endpoint.getIp(),
                    endpoint.getPort(),
                    connectionTimeout))));
    this.endpoint = endpoint;
    this.clientManager = clientManager;
    getInputProtocol().getTransport().open();
  }

  public void returnSelf() {
    if (clientManager != null) {
      clientManager.returnClient(endpoint, this);
    }
  }

  public void setTimeout(int timeout) {
    // the same transport is used in both input and output
    ((TimeoutChangeableTransport) (getInputProtocol().getTransport())).setTimeout(timeout);
  }

  public void close() {
    getInputProtocol().getTransport().close();
  }

  public int getTimeout() throws SocketException {
    return ((TimeoutChangeableTransport) getInputProtocol().getTransport()).getTimeOut();
  }

  public static class Factory extends BaseClientFactory<EndPoint, SyncConfigNodeIServiceClient> {

    public Factory(
        ClientManager<EndPoint, SyncConfigNodeIServiceClient> clientManager,
        ClientManagerProperty<SyncConfigNodeIServiceClient> clientManagerProperty) {
      super(clientManager, clientManagerProperty);
    }

    @Override
    public void destroyObject(
        EndPoint endpoint, PooledObject<SyncConfigNodeIServiceClient> pooledObject) {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<SyncConfigNodeIServiceClient> makeObject(EndPoint endpoint)
        throws Exception {
      return new DefaultPooledObject<>(
          new SyncConfigNodeIServiceClient(
              clientManagerProperty.getProtocolFactory(),
              clientManagerProperty.getConnectionTimeoutMs(),
              endpoint,
              clientManager));
    }

    @Override
    public boolean validateObject(
        EndPoint endpoint, PooledObject<SyncConfigNodeIServiceClient> pooledObject) {
      return pooledObject.getObject() != null
          && pooledObject.getObject().getInputProtocol().getTransport().isOpen();
    }
  }
}
