/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.milvus.v2.utils;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.grpc.netty.shaded.io.netty.handler.ssl.JdkSslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.stub.MetadataUtils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.GetVersionRequest;
import io.milvus.grpc.GetVersionResponse;
import io.milvus.grpc.ListDatabasesRequest;
import io.milvus.grpc.ListDatabasesResponse;
import io.milvus.grpc.MilvusServiceGrpc;
import io.milvus.v2.client.ConnectConfig;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientUtils {
  Logger logger = LoggerFactory.getLogger(ClientUtils.class);
  RpcUtils rpcUtils = new RpcUtils();

  public ClientUtils() {}

  public ManagedChannel getChannel(ConnectConfig connectConfig) {
    ManagedChannel channel = null;
    Metadata metadata = new Metadata();
    if (connectConfig.getAuthorization() != null) {
      metadata.put(
          Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
          Base64.getEncoder()
              .encodeToString(connectConfig.getAuthorization().getBytes(StandardCharsets.UTF_8)));
    }

    if (StringUtils.isNotEmpty(connectConfig.getDbName())) {
      metadata.put(Key.of("dbname", Metadata.ASCII_STRING_MARSHALLER), connectConfig.getDbName());
    }

    try {
      if (connectConfig.getSslContext() != null) {
        NettyChannelBuilder builder =
            (NettyChannelBuilder)
                ((NettyChannelBuilder)
                        ((NettyChannelBuilder)
                                NettyChannelBuilder.forAddress(
                                        connectConfig.getHost(), connectConfig.getPort())
                                    .overrideAuthority(connectConfig.getServerName()))
                            .sslContext(convertJavaSslContextToNetty(connectConfig))
                            .maxInboundMessageSize(Integer.MAX_VALUE)
                            .keepAliveTime(
                                connectConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                            .keepAliveTimeout(
                                connectConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                            .keepAliveWithoutCalls(connectConfig.isKeepAliveWithoutCalls())
                            .idleTimeout(connectConfig.getIdleTimeoutMs(), TimeUnit.MILLISECONDS))
                    .intercept(
                        new ClientInterceptor[] {
                          MetadataUtils.newAttachHeadersInterceptor(metadata)
                        });
        if (connectConfig.isSecure()) {
          builder.useTransportSecurity();
        }

        if (StringUtils.isNotEmpty(connectConfig.getServerName())) {
          builder.overrideAuthority(connectConfig.getServerName());
        }

        channel = builder.build();
      } else {
        NettyChannelBuilder builder;
        SslContext sslContext;
        if (StringUtils.isNotEmpty(connectConfig.getServerPemPath())) {
          sslContext =
              GrpcSslContexts.forClient()
                  .trustManager(new File(connectConfig.getServerPemPath()))
                  .build();
          builder =
              (NettyChannelBuilder)
                  ((NettyChannelBuilder)
                          ((NettyChannelBuilder)
                                  NettyChannelBuilder.forAddress(
                                          connectConfig.getHost(), connectConfig.getPort())
                                      .overrideAuthority(connectConfig.getServerName()))
                              .sslContext(sslContext)
                              .maxInboundMessageSize(Integer.MAX_VALUE)
                              .keepAliveTime(
                                  connectConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                              .keepAliveTimeout(
                                  connectConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                              .keepAliveWithoutCalls(connectConfig.isKeepAliveWithoutCalls())
                              .idleTimeout(connectConfig.getIdleTimeoutMs(), TimeUnit.MILLISECONDS))
                      .intercept(
                          new ClientInterceptor[] {
                            MetadataUtils.newAttachHeadersInterceptor(metadata)
                          });
          if (connectConfig.isSecure()) {
            builder.useTransportSecurity();
          }

          channel = builder.build();
        } else if (StringUtils.isNotEmpty(connectConfig.getClientPemPath())
            && StringUtils.isNotEmpty(connectConfig.getClientKeyPath())
            && StringUtils.isNotEmpty(connectConfig.getCaPemPath())) {
          sslContext =
              GrpcSslContexts.forClient()
                  .trustManager(new File(connectConfig.getCaPemPath()))
                  .keyManager(
                      new File(connectConfig.getClientPemPath()),
                      new File(connectConfig.getClientKeyPath()))
                  .build();
          builder =
              (NettyChannelBuilder)
                  ((NettyChannelBuilder)
                          NettyChannelBuilder.forAddress(
                                  connectConfig.getHost(), connectConfig.getPort())
                              .sslContext(sslContext)
                              .maxInboundMessageSize(Integer.MAX_VALUE)
                              .keepAliveTime(
                                  connectConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                              .keepAliveTimeout(
                                  connectConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                              .keepAliveWithoutCalls(connectConfig.isKeepAliveWithoutCalls())
                              .idleTimeout(connectConfig.getIdleTimeoutMs(), TimeUnit.MILLISECONDS))
                      .intercept(
                          new ClientInterceptor[] {
                            MetadataUtils.newAttachHeadersInterceptor(metadata)
                          });
          if (connectConfig.getSecure()) {
            builder.useTransportSecurity();
          }

          if (StringUtils.isNotEmpty(connectConfig.getServerName())) {
            builder.overrideAuthority(connectConfig.getServerName());
          }

          channel = builder.build();
        } else {
          ManagedChannelBuilder<?> builder1 =
              ManagedChannelBuilder.forAddress(connectConfig.getHost(), connectConfig.getPort())
                  .usePlaintext()
                  .maxInboundMessageSize(Integer.MAX_VALUE)
                  .keepAliveTime(connectConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS)
                  .keepAliveTimeout(connectConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS)
                  .keepAliveWithoutCalls(connectConfig.isKeepAliveWithoutCalls())
                  .idleTimeout(connectConfig.getIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                  .intercept(
                      new ClientInterceptor[] {
                        MetadataUtils.newAttachHeadersInterceptor(metadata)
                      });
          if (connectConfig.isSecure()) {
            builder1.useTransportSecurity();
          }

          channel = builder1.build();
        }
      }
    } catch (IOException var6) {
      this.logger.error("Failed to open credentials file, error:{}\n", var6.getMessage());
    }

    assert channel != null;

    return channel;
  }

  private static JdkSslContext convertJavaSslContextToNetty(ConnectConfig connectConfig) {
    ApplicationProtocolConfig applicationProtocolConfig =
        new ApplicationProtocolConfig(
            Protocol.NONE,
            SelectorFailureBehavior.FATAL_ALERT,
            SelectedListenerFailureBehavior.FATAL_ALERT,
            new String[0]);
    return new JdkSslContext(
        connectConfig.getSslContext(),
        true,
        (Iterable) null,
        IdentityCipherSuiteFilter.INSTANCE,
        applicationProtocolConfig,
        ClientAuth.NONE,
        (String[]) null,
        false);
  }

  public void checkDatabaseExist(
      MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub, String dbName) {
    String title = String.format("Check database %s exist", dbName);
    ListDatabasesRequest listDatabasesRequest = ListDatabasesRequest.newBuilder().build();
    ListDatabasesResponse response = blockingStub.listDatabases(listDatabasesRequest);
    this.rpcUtils.handleResponse(title, response.getStatus());
    if (!response.getDbNamesList().contains(dbName)) {
      throw new IllegalArgumentException("Database " + dbName + " not exist");
    }
  }

  public String getServerVersion(MilvusServiceGrpc.MilvusServiceBlockingStub blockingStub) {
    GetVersionResponse response = blockingStub.getVersion(GetVersionRequest.newBuilder().build());
    this.rpcUtils.handleResponse("Get server version", response.getStatus());
    return response.getVersion();
  }

  public String getHostName() {
    try {
      InetAddress address = InetAddress.getLocalHost();
      return address.getHostName();
    } catch (UnknownHostException var2) {
      this.logger.warn("Failed to get host name, error:{}\n", var2.getMessage());
      return "Unknown";
    }
  }

  public String getLocalTimeStr() {
    LocalDateTime now = LocalDateTime.now();
    return now.toString();
  }

  public String getSDKVersion() {
    Package pkg = MilvusServiceClient.class.getPackage();
    if (pkg == null) {
      return "";
    }
    String ver = pkg.getImplementationVersion();
    return ver == null ? "" : ver;
  }
}
