/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.ratis;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.ozone.OzoneConfigKeys;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.retry.RequestTypeDependentRetryPolicy;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.retry.*;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis helper methods.
 */
public interface RatisHelper {
  Logger LOG = LoggerFactory.getLogger(RatisHelper.class);

  // Prefix for Ratis Server GRPC and Ratis client conf.
  String HDDS_DATANODE_RATIS_PREFIX_KEY = "hdds.ratis.";
  String RAFT_SERVER_PREFIX_KEY = "raft.server";
  String HDDS_DATANODE_RATIS_SERVER_PREFIX_KEY =
      HDDS_DATANODE_RATIS_PREFIX_KEY + RAFT_SERVER_PREFIX_KEY;
  String HDDS_DATANODE_RATIS_CLIENT_PREFIX_KEY =
      HDDS_DATANODE_RATIS_PREFIX_KEY + RaftClientConfigKeys.PREFIX;
  String HDDS_DATANODE_RATIS_GRPC_PREFIX_KEY =
      HDDS_DATANODE_RATIS_PREFIX_KEY + GrpcConfigKeys.PREFIX;


  static String toRaftPeerIdString(DatanodeDetails id) {
    return id.getUuidString();
  }

  static UUID toDatanodeId(String peerIdString) {
    return UUID.fromString(peerIdString);
  }

  static UUID toDatanodeId(RaftPeerId peerId) {
    return toDatanodeId(peerId.toString());
  }

  static UUID toDatanodeId(RaftProtos.RaftPeerProto peerId) {
    return toDatanodeId(RaftPeerId.valueOf(peerId.getId()));
  }

  static String toRaftPeerAddressString(DatanodeDetails id) {
    return id.getIpAddress() + ":" +
        id.getPort(DatanodeDetails.Port.Name.RATIS).getValue();
  }

  static RaftPeerId toRaftPeerId(DatanodeDetails id) {
    return RaftPeerId.valueOf(toRaftPeerIdString(id));
  }

  static RaftPeer toRaftPeer(DatanodeDetails id) {
    return new RaftPeer(toRaftPeerId(id), toRaftPeerAddressString(id));
  }

  static List<RaftPeer> toRaftPeers(Pipeline pipeline) {
    return toRaftPeers(pipeline.getNodes());
  }

  static <E extends DatanodeDetails> List<RaftPeer> toRaftPeers(
      List<E> datanodes) {
    return datanodes.stream().map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
  }

  /* TODO: use a dummy id for all groups for the moment.
   *       It should be changed to a unique id for each group.
   */
  RaftGroupId DUMMY_GROUP_ID =
      RaftGroupId.valueOf(ByteString.copyFromUtf8("AOzoneRatisGroup"));

  RaftGroup EMPTY_GROUP = RaftGroup.valueOf(DUMMY_GROUP_ID,
      Collections.emptyList());

  static RaftGroup emptyRaftGroup() {
    return EMPTY_GROUP;
  }

  static RaftGroup newRaftGroup(Collection<RaftPeer> peers) {
    return peers.isEmpty()? emptyRaftGroup()
        : RaftGroup.valueOf(DUMMY_GROUP_ID, peers);
  }

  static RaftGroup newRaftGroup(RaftGroupId groupId,
      Collection<DatanodeDetails> peers) {
    final List<RaftPeer> newPeers = peers.stream()
        .map(RatisHelper::toRaftPeer)
        .collect(Collectors.toList());
    return peers.isEmpty() ? RaftGroup.valueOf(groupId, Collections.emptyList())
        : RaftGroup.valueOf(groupId, newPeers);
  }

  static RaftGroup newRaftGroup(Pipeline pipeline) {
    return RaftGroup.valueOf(RaftGroupId.valueOf(pipeline.getId().getId()),
        toRaftPeers(pipeline));
  }

  static RaftClient newRaftClient(RpcType rpcType, Pipeline pipeline,
      RetryPolicy retryPolicy, GrpcTlsConfig tlsConfig,
      ConfigurationSource ozoneConfiguration) throws IOException {
    return newRaftClient(rpcType,
        toRaftPeerId(pipeline.getLeaderNode()),
        newRaftGroup(RaftGroupId.valueOf(pipeline.getId().getId()),
            pipeline.getNodes()), retryPolicy, tlsConfig, ozoneConfiguration);
  }

  static RpcType getRpcType(ConfigurationSource conf) {
    return SupportedRpcType.valueOfIgnoreCase(conf.get(
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_KEY,
        ScmConfigKeys.DFS_CONTAINER_RATIS_RPC_TYPE_DEFAULT));
  }

  static RaftClient newRaftClient(RaftPeer leader, ConfigurationSource conf) {
    return newRaftClient(getRpcType(conf), leader,
        RatisHelper.createRetryPolicy(conf), conf);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy, GrpcTlsConfig tlsConfig,
      ConfigurationSource configuration) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(Collections.singletonList(leader)), retryPolicy,
        tlsConfig, configuration);
  }

  static RaftClient newRaftClient(RpcType rpcType, RaftPeer leader,
      RetryPolicy retryPolicy,
      ConfigurationSource ozoneConfiguration) {
    return newRaftClient(rpcType, leader.getId(),
        newRaftGroup(Collections.singletonList(leader)), retryPolicy, null,
        ozoneConfiguration);
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  static RaftClient newRaftClient(RpcType rpcType, RaftPeerId leader,
      RaftGroup group, RetryPolicy retryPolicy,
      GrpcTlsConfig tlsConfig, ConfigurationSource ozoneConfiguration) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("newRaftClient: {}, leader={}, group={}",
          rpcType, leader, group);
    }
    final RaftProperties properties = new RaftProperties();

    RaftConfigKeys.Rpc.setType(properties, rpcType);

    // Set the ratis client headers which are matching with regex.
    createRaftClientProperties(ozoneConfiguration, properties);

    RaftClient.Builder builder =  RaftClient.newBuilder()
        .setRaftGroup(group)
        .setLeaderId(leader)
        .setProperties(properties)
        .setRetryPolicy(retryPolicy);

    // TODO: GRPC TLS only for now, netty/hadoop RPC TLS support later.
    if (tlsConfig != null && rpcType == SupportedRpcType.GRPC) {
      builder.setParameters(GrpcFactory.newRaftParameters(tlsConfig));
    }
    return builder.build();
  }

  /**
   * Set all the properties matching with regex
   * {@link RatisHelper#HDDS_DATANODE_RATIS_PREFIX_KEY} in
   * ozone configuration object and configure it to RaftProperties.
   * @param ozoneConf
   * @param raftProperties
   */
  static void createRaftClientProperties(ConfigurationSource ozoneConf,
      RaftProperties raftProperties) {

    // As for client we do not require server and grpc server/tls. exclude them.
    Map<String, String> ratisClientConf =
        ozoneConf.getPropsWithPrefix(HDDS_DATANODE_RATIS_PREFIX_KEY);
    ratisClientConf.forEach((key, val) -> {
      if (!(key.startsWith(RAFT_SERVER_PREFIX_KEY) ||
          key.startsWith(GrpcConfigKeys.TLS.PREFIX) ||
          key.startsWith(GrpcConfigKeys.Server.PREFIX))) {
        raftProperties.set(key, val);
      }
    });
  }


  /**
   * Set all the properties matching with prefix
   * {@link RatisHelper#HDDS_DATANODE_RATIS_PREFIX_KEY} in
   * ozone configuration object and configure it to RaftProperties.
   * @param ozoneConf
   * @param raftProperties
   */
  static void createRaftServerProperties(ConfigurationSource ozoneConf,
       RaftProperties raftProperties) {

    Map<String, String> ratisServerConf =
        getDatanodeRatisPrefixProps(ozoneConf);
    ratisServerConf.forEach((key, val) -> {
      // Exclude ratis client configuration.
      if (!key.startsWith(RaftClientConfigKeys.PREFIX)) {
        raftProperties.set(key, val);
      }
    });
  }


  static Map<String, String> getDatanodeRatisPrefixProps(
      ConfigurationSource configuration) {
    return configuration.getPropsWithPrefix(HDDS_DATANODE_RATIS_PREFIX_KEY);
  }

  // For External gRPC client to server with gRPC TLS.
  // No mTLS for external client as SCM CA does not issued certificates for them
  static GrpcTlsConfig createTlsClientConfig(SecurityConfig conf,
      X509Certificate caCert) {
    GrpcTlsConfig tlsConfig = null;
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      tlsConfig = new GrpcTlsConfig(null, null,
          caCert, false);
    }
    return tlsConfig;
  }

  static RetryPolicy createRetryPolicy(ConfigurationSource conf) {
    long exponentialBaseSleep = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_EXPONENTIAL_BACKOFF_BASE_SLEEP,
        OzoneConfigKeys.DFS_RATIS_CLIENT_EXPONENTIAL_BACKOFF_BASE_SLEEP_DEFAULT
        .toIntExact(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    long exponentialMaxSleep = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_EXPONENTIAL_BACKOFF_MAX_SLEEP,
        OzoneConfigKeys.
            DFS_RATIS_CLIENT_EXPONENTIAL_BACKOFF_MAX_SLEEP_DEFAULT
            .toIntExact(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    ExponentialBackoffRetry exponentialBackoffRetry =
        ExponentialBackoffRetry.newBuilder().setBaseSleepTime(
            TimeDuration.valueOf(exponentialBaseSleep, TimeUnit.MILLISECONDS))
            .setMaxSleepTime(TimeDuration.valueOf(exponentialMaxSleep, TimeUnit.MILLISECONDS))
            .build();

    MultipleLinearRandomRetry multipleLinearRandomRetry =
        MultipleLinearRandomRetry.parseCommaSeparated(conf.get(
            OzoneConfigKeys.DFS_RATIS_CLIENT_MULTILINEAR_RANDOM_RETRY_POLICY,
            OzoneConfigKeys.DFS_RATIS_CLIENT_MULTILINEAR_RANDOM_RETRY_POLICY_DEFAULT));

    ExceptionDependentRetry.Builder exceptionDependentBuilder =
        ExceptionDependentRetry.newBuilder()
            .setExceptionToPolicy(NotReplicatedException.class,
                RetryPolicies.noRetry())
            .setExceptionToPolicy(GroupMismatchException.class,
                RetryPolicies.noRetry())
            .setExceptionToPolicy(StateMachineException.class,
                RetryPolicies.noRetry())
            .setExceptionToPolicy(ResourceUnavailableException.class,
                exponentialBackoffRetry)
            .setDefaultPolicy(multipleLinearRandomRetry);

    ExceptionDependentRetry.Builder exceptionDependentBuilder2 =
        ExceptionDependentRetry.newBuilder()
            .setExceptionToPolicy(NotReplicatedException.class,
                RetryPolicies.noRetry())
            .setExceptionToPolicy(GroupMismatchException.class,
                RetryPolicies.noRetry())
            .setExceptionToPolicy(StateMachineException.class,
                RetryPolicies.noRetry())
            .setExceptionToPolicy(ResourceUnavailableException.class,
                exponentialBackoffRetry)
            .setDefaultPolicy(multipleLinearRandomRetry);

    long writeTimeout = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_WRITE_TIMEOUT, OzoneConfigKeys.
            DFS_RATIS_CLIENT_REQUEST_WRITE_TIMEOUT_DEFAULT
            .toIntExact(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    long watchTimeout = conf.getTimeDuration(
        OzoneConfigKeys.DFS_RATIS_CLIENT_REQUEST_WATCH_TIMEOUT, OzoneConfigKeys.
            DFS_RATIS_CLIENT_REQUEST_WATCH_TIMEOUT_DEFAULT
            .toIntExact(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    return RequestTypeDependentRetryPolicy.newBuilder()
        .setRetryPolicy(RaftProtos.RaftClientRequestProto.TypeCase.WRITE,
            exceptionDependentBuilder
                .setExceptionToPolicy(TimeoutIOException.class,
                    exponentialBackoffRetry).build())
        .setRetryPolicy(RaftProtos.RaftClientRequestProto.TypeCase.WATCH,
            exceptionDependentBuilder2
                .setExceptionToPolicy(TimeoutIOException.class, RetryPolicies.noRetry()).build())
        .setTimeout(RaftProtos.RaftClientRequestProto.TypeCase.WRITE,
            TimeDuration.valueOf(writeTimeout, TimeUnit.MILLISECONDS))
        .setTimeout(RaftProtos.RaftClientRequestProto.TypeCase.WATCH,
            TimeDuration.valueOf(watchTimeout, TimeUnit.MILLISECONDS))
        .build();
  }

  static Long getMinReplicatedIndex(
      Collection<RaftProtos.CommitInfoProto> commitInfos) {
    return commitInfos.stream().map(RaftProtos.CommitInfoProto::getCommitIndex)
        .min(Long::compareTo).orElse(null);
  }
}
