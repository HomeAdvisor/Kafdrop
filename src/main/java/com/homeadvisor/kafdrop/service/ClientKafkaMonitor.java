/*
 * Copyright 2019 HomeAdvisor, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.homeadvisor.kafdrop.service;

import com.google.common.base.Throwables;
import com.homeadvisor.kafdrop.config.KafkaConfiguration;
import com.homeadvisor.kafdrop.model.*;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.homeadvisor.kafdrop.config.KafkaConfiguration.DEFAULT_REQUEST_TIMEOUT_MS;

// todo: opportunities for RxJava streams abound here.
@Service
@Primary
public class ClientKafkaMonitor implements KafkaMonitor
{
   private final Logger logger = LoggerFactory.getLogger(getClass());

   private int requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS;

   private final ObjectPool<AdminClient> adminClientPool;
   private final ObjectPool<Consumer> consumerPool;

   private final BrokerRegistry brokerRegistry;

   @Autowired
   public ClientKafkaMonitor(KafkaConfiguration kafkaConfiguration, BrokerRegistry brokerRegistry)
   {
      this.brokerRegistry = brokerRegistry;

      if (kafkaConfiguration.getRequestTimeoutMs() > 0)
      {
         this.requestTimeoutMs = kafkaConfiguration.getRequestTimeoutMs();
      }
      else
      {
         logger.warn("Request timeout {} is invalid. Must be greater than 0. Using default of {}",
                     kafkaConfiguration.getRequestTimeoutMs(), DEFAULT_REQUEST_TIMEOUT_MS);
      }

      GenericObjectPoolConfig<AdminClient> adminPoolConfig = new GenericObjectPoolConfig<>();
      applyPoolProperties(adminPoolConfig, kafkaConfiguration.getAdminPool());
      adminClientPool = new GenericObjectPool<>(new AdminClientObjectFactory(kafkaConfiguration), adminPoolConfig);

      GenericObjectPoolConfig<Consumer> consumerPoolConfig = new GenericObjectPoolConfig<>();
      applyPoolProperties(consumerPoolConfig, kafkaConfiguration.getConsumerPool());
      consumerPool = new GenericObjectPool<>(new ConsumerObjectFactory(kafkaConfiguration), consumerPoolConfig);
   }

   public void applyPoolProperties(GenericObjectPoolConfig<?> poolConfig,
                                   KafkaConfiguration.PoolProperties poolProperties)
   {
      poolConfig.setMinIdle(poolProperties.getMinIdle());
      poolConfig.setMaxIdle(poolProperties.getMaxIdle());
      poolConfig.setMaxTotal(poolProperties.getMaxTotal());
      poolConfig.setMaxWaitMillis(poolProperties.getMaxWaitMillis());
   }

   @PreDestroy
   public void close()
   {
      adminClientPool.close();
   }

   @Override
   public List<BrokerVO> getBrokers()
   {

      DescribeClusterResult cluster = admin(AdminClient::describeCluster);

      Collection<Node> nodeList = waitOnFuture(cluster.nodes());
      if (nodeList.isEmpty())
      {
         return Collections.emptyList();
      }
      else
      {
         return createBrokers(nodeList, waitOnFuture(cluster.controller()));
      }
   }

   public List<BrokerVO> createBrokers(Collection<Node> nodeList, Node controller)
   {
      return nodeList.stream()
         .map(node -> createBroker(node, controller.id() == node.id()))
         .sorted(Comparator.comparing(BrokerVO::getId))
         .collect(Collectors.toList());
   }

   private BrokerVO createBroker(Node node, boolean controller)
   {
      BrokerVO broker = new BrokerVO();
      broker.setId(node.id());
      broker.setController(controller);
      broker.setHost(node.host());
      broker.setPort(node.port());
      broker.setRack(node.rack());

      Optional.ofNullable(brokerRegistry)
         .map(BrokerRegistry::getBrokers)
         .map(brokers -> brokers.get(node.id()))
         .ifPresent(cachedBroker -> {
            broker.setTimestamp(cachedBroker.getTimestamp());
            broker.setJmxPort(cachedBroker.getJmxPort());
            broker.setVersion(cachedBroker.getVersion());
         });

      return broker;
   }

   @Override
   public Optional<BrokerVO> getBroker(int id)
   {
      return getBrokers().stream()
         .filter(broker -> broker.getId() == id)
         .findFirst();
   }

   @Override
   public Optional<BrokerConfigurationVO> getBrokerConfiguration(int id)
   {
      ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id));

      DescribeConfigsResult configs = admin(client -> {
         return client.describeConfigs(
            Collections.singletonList(configResource));
      });

      return Optional.ofNullable(waitOnFuture(configs.all()).get(configResource))
         .map(config -> createBrokerConfiguration(id, config));
   }

   private BrokerConfigurationVO createBrokerConfiguration(int id, Config config)
   {
      return new BrokerConfigurationVO(id,
                                       config.entries().stream()
                                          .map(this::createBrokerConfigEntry)
                                          .collect(Collectors.toList()));
   }

   private BrokerConfigurationVO.ConfigEntryVO createBrokerConfigEntry(ConfigEntry entry)
   {
      return new BrokerConfigurationVO.ConfigEntryVO(entry.name(),
                                                     entry.value(),
                                                     entry.source().name(),
                                                     entry.isSensitive());
   }

   @Override
   public Optional<TopicVO> getTopic(String topic)
   {
      return getTopicDetail(Collections.singletonList(topic))
         .stream()
         .findFirst();
   }

   @Override
   public List<TopicVO> getTopics()
   {
      ListTopicsOptions options = new ListTopicsOptions().listInternal(true);
      ListTopicsResult topicList = admin(a -> a.listTopics(options));
      return getTopicDetail(waitOnFuture(topicList.names()));
   }

   private List<TopicVO> getTopicDetail(Collection<String> topics)
   {

      return getTopicDetailMap(topics)
         .values().stream()
         .sorted(Comparator.comparing(TopicVO::getName))
         .collect(Collectors.toList());
   }

   private void populateTopicOffsets(Map<String, TopicVO> topicMap)
   {
      try
      {
         List<TopicPartition> partitions = topicMap.values().stream()
            .flatMap(t -> t.getPartitions().stream()
               .map(p -> new TopicPartition(t.getName(), p.getId())))
            .collect(Collectors.toList());


            logger.trace("Requesting first offsets for {}", partitions);
            Map<TopicPartition, Long> firstOffsets = consumer(c -> {
               Map<TopicPartition, Long> map = c.beginningOffsets(partitions);
               return map;
            });

            logger.trace("Requesting end offsets for {}", partitions);
            Map<TopicPartition, Long> endOffsets = consumer(c -> {
               Map<TopicPartition, Long> map = c.endOffsets(partitions);
               return map;
            });


            firstOffsets.forEach(
               (tp, offset) ->
                  topicMap.get(tp.topic())
                     .getPartition(tp.partition())
                     .ifPresent(p -> p.setFirstOffset(offset)));
            endOffsets.forEach(
               (tp, offset) ->
                  topicMap.get(tp.topic())
                     .getPartition(tp.partition())
                     .ifPresent(p -> p.setSize(offset)));
      }
      catch (org.apache.kafka.common.errors.TimeoutException e)
      {
         logger.warn("Unable to obtain topic offset information", e);
      }
   }

   private Map<String, TopicVO> getTopicDetailMap(Collection<String> topics)
   {
      Map<String, Config> configMap = getTopicConfigs(topics);

      DescribeTopicsResult topicDetails = admin(a -> a.describeTopics(topics));
      Map<String, TopicVO> topicDetailMap = waitOnFuture(topicDetails.all())
         .values().stream()
         .map(description -> createTopic(description, configMap.get(description.name())))
         .collect(Collectors.toMap(TopicVO::getName, t -> t));

      populateTopicOffsets(topicDetailMap);

      return topicDetailMap;
   }

   private Map<String, Config> getTopicConfigs(Collection<String> topics)
   {
      DescribeConfigsResult topicConfigs =
         admin(a -> a.describeConfigs(
            topics.stream()
               .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
               .collect(Collectors.toList())
               )
         );

      return waitOnFuture(topicConfigs.all())
         .entrySet().stream()
         .collect(Collectors.toMap(e -> e.getKey().name(), Map.Entry::getValue));
   }

   private TopicVO createTopic(TopicDescription description, Config config)
   {
      TopicVO topic = new TopicVO(description.name());
//      IntStream.range(description.)
      topic.setPartitions(
         description.partitions().stream()
            .map(this::createTopicPartition)
            .collect(Collectors.toMap(TopicPartitionVO::getId, tp -> tp))
      );

      topic.setConfig(config.entries().stream()
                         .filter(ce ->
                                    ce.source() != ConfigEntry.ConfigSource.DEFAULT_CONFIG &&
                                    ce.source() != ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
                         .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)));

      return topic;
   }

   private TopicPartitionVO createTopicPartition(TopicPartitionInfo partition)
   {
      TopicPartitionVO topicPartition = new TopicPartitionVO(partition.partition());
      partition.replicas()
         .forEach(replica -> topicPartition.addReplica(
            new TopicPartitionVO.PartitionReplica(replica.id(),
                                                  partition.isr().contains(replica),
                                                  partition.leader().id() == replica.id())));
      return topicPartition;
   }

   @Override
   public ClusterSummaryVO getClusterSummary(Collection<TopicVO> topics)
   {
      return new ClusterSummaryFactory().getClusterSummary(topics);
   }

   @Override
   public List<ConsumerVO> getConsumers(TopicVO topic)
   {
      // todo: rxjava this
      ListConsumerGroupsResult groupsResult = admin(AdminClient::listConsumerGroups);
      List<String> groupIds = waitOnFuture(groupsResult.all().thenApply(groups -> groups.stream()
         .map(ConsumerGroupListing::groupId)
         .collect(Collectors.toList()))
      );


      return getConsumers(groupIds, topic);
   }

   public List<ConsumerVO> getConsumers(List<String> groupIds, TopicVO topic)
   {
      Map<String, ConsumerGroupDescription> groupDescriptionMap =
         waitOnFuture(admin(a -> a.describeConsumerGroups(groupIds)).all());

      final Map<String, TopicVO> topicDetailMap;
      if (topic != null)
      {
         topicDetailMap = Collections.singletonMap(topic.getName(), topic);
      }
      else
      {
         topicDetailMap = getTopicDetailsForConsumers(groupDescriptionMap);
      }


      List<ConsumerVO> consumers =
         groupDescriptionMap.values().stream()
            .filter(consumerGroup -> topic == null || groupSubscribesToTopic(consumerGroup, topic.getName()))
            .map(groupDescription -> createConsumer(groupDescription, topicDetailMap))
            .collect(Collectors.toList());

      return consumers;
   }

   public Map<String, TopicVO> getTopicDetailsForConsumers(Map<String, ConsumerGroupDescription> groupDescriptionMap)
   {
      Map<String, TopicVO> topicDetailMap;
      topicDetailMap = getTopicDetailMap(
         groupDescriptionMap.values().stream()
            .map(ConsumerGroupDescription::members)
            .flatMap(Collection::stream)
            .map(MemberDescription::assignment)
            .map(MemberAssignment::topicPartitions)
            .flatMap(Collection::stream)
            .map(TopicPartition::topic)
            .collect(Collectors.toList()));
      return topicDetailMap;
   }

   private void populateConsumerOffsetsFromTopic(ConsumerVO consumer, TopicVO topic)
   {
      topic.getPartitions().forEach(topicPartition ->
                                       populatePartitionOffsetBounds(consumer.getOrCreateTopic(topic.getName())
                                                                        .getOrCreatePartition(topicPartition.getId()),
                                                                     topicPartition)
      );
   }

   private void populatePartitionOffsetBounds(ConsumerPartitionVO partition,
                                              TopicPartitionVO topicPartition)
   {
      partition.setFirstOffset(topicPartition.getFirstOffset());
      partition.setSize(topicPartition.getSize());
   }


   private boolean groupSubscribesToTopic(ConsumerGroupDescription groupDescription, String topic)
   {
      return groupDescription.members().stream()
         .anyMatch(m -> m.assignment().topicPartitions().stream()
            .anyMatch(tp -> tp.topic().equals(topic)));
   }

   private ConsumerVO createConsumer(ConsumerGroupDescription groupDescription,
                                     Map<String, TopicVO> topicDetailMap)
   {
      ConsumerVO vo = new ConsumerVO(groupDescription.groupId());

//      topicDetailMap.values().forEach(topic -> vo.getOrCreateTopic(topic.getName()));

      addConsumerRegistrations(vo, groupDescription);
      addConsumerTopics(vo, groupDescription, topicDetailMap);
      populateConsumerPartitions(vo, groupDescription);
      assignPartitionOwners(vo, groupDescription);

      return vo;
   }

   private void addConsumerRegistrations(ConsumerVO vo, ConsumerGroupDescription groupDescription)
   {
      groupDescription.members().stream()
         .map(this::createConsumerRegistration)
         .forEach(vo::addActiveInstance);
   }

   private void populateConsumerPartitions(ConsumerVO vo, ConsumerGroupDescription groupDescription)
   {
      String groupId = groupDescription.groupId();

      waitOnFuture(
         admin(a -> a.listConsumerGroupOffsets(groupId)
            .partitionsToOffsetAndMetadata()))
         .forEach((key, value) -> vo.getOrCreateTopic(key.topic())
            .getOrCreatePartition(key.partition())
            .setConsumerOffset(new ConsumerOffsetVO(-1, value.offset())));
   }

   private void addConsumerTopics(ConsumerVO vo,
                                  ConsumerGroupDescription groupDescription,
                                  Map<String, TopicVO> topicDetailMap)
   {
      topicDetailMap.values().forEach(topic -> populateConsumerOffsetsFromTopic(vo, topic));
   }

   private void assignPartitionOwners(ConsumerVO vo, ConsumerGroupDescription groupDescription)
   {
      groupDescription.members()
         .forEach(
            member -> member.assignment()
               .topicPartitions()
               .forEach(
                  tp -> vo.getOrCreateTopic(tp.topic())
                     .getOrCreatePartition(tp.partition())
                     .setOwner(member.consumerId()))
               );
   }

   private ConsumerPartitionVO createConsumerPartition(String groupId,
                                                       TopicPartition topicPartition,
                                                       OffsetAndMetadata offset)
   {
      ConsumerPartitionVO vo = new ConsumerPartitionVO(groupId, topicPartition.topic(), topicPartition.partition());
      vo.setConsumerOffset(new ConsumerOffsetVO(-1, offset.offset()));
      return vo;
   }

   private ConsumerRegistrationVO createConsumerRegistration(MemberDescription member)
   {
      ConsumerRegistrationVO registration = new ConsumerRegistrationVO(member.consumerId());
      registration.setSubscriptions(
         member.assignment().topicPartitions().stream()
            .collect(Collectors.toMap(TopicPartition::topic, v -> 1, Integer::sum))
      );
      return registration;
   }

   @Override
   public Optional<ConsumerVO> getConsumer(String groupId)
   {
      return getConsumerByTopic(groupId, null);
   }

   @Override
   public Optional<ConsumerVO> getConsumerByTopic(String groupId, TopicVO topic)
   {
      return getConsumers(Collections.singletonList(groupId), topic)
         .stream()
         .findFirst();
   }

   private <T> T waitOnFuture(Future<T> future)
   {
      try
      {
         return future.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e)
      {
         Thread.currentThread().interrupt();
         throw Throwables.propagate(e);
      }
      catch (ExecutionException e)
      {
         throw Throwables.propagate(e.getCause());
      }
      catch (TimeoutException e)
      {
         throw Throwables.propagate(e);
      }
   }

   private <T> T admin(Function<AdminClient, T> callback)
   {
      return withPoolObject(adminClientPool, callback);
   }

   private <T> T consumer(Function<Consumer, T> callback)
   {
      return withPoolObject(consumerPool, callback);
   }

   private <O, P extends ObjectPool<O>, T> T withPoolObject(P pool, Function<O, T> callback)
   {
      O obj = null;
      try
      {
         obj = pool.borrowObject();
         return callback.apply(obj);
      }
      catch (Exception ex)
      {
         throw Throwables.propagate(ex);
      }
      finally
      {
         try
         {
            if (obj != null)
            {
               pool.returnObject(obj);
            }
         }
         catch (Exception ex)
         {
            logger.debug("Unable to return {} to pool", obj.getClass().getSimpleName(), ex);
         }
      }
   }

   /**
    * Creates AdminClient instances since the client is not thread safe
    */
   private static class AdminClientObjectFactory extends BasePooledObjectFactory<AdminClient>
   {
      private final KafkaConfiguration config;

      public AdminClientObjectFactory(KafkaConfiguration config)
      {
         this.config = config;
      }

      @Override
      public AdminClient create() throws Exception
      {
         Properties adminProperties = new Properties();
         config.applyCommon(adminProperties);
         return KafkaAdminClient.create(adminProperties);
      }

      @Override
      public PooledObject<AdminClient> wrap(AdminClient obj)
      {
         return new DefaultPooledObject<>(obj);
      }

      @Override
      public void destroyObject(PooledObject<AdminClient> p) throws Exception
      {
         p.getObject().close(Duration.ofMillis(5_000));
      }
   }


   /**
    * Creates Consumer instances since the KafkaConsumer is not thread safe
    */
   private static class ConsumerObjectFactory extends BasePooledObjectFactory<Consumer>
   {
      private final KafkaConfiguration config;

      public ConsumerObjectFactory(KafkaConfiguration config)
      {
         this.config = config;
      }

      @Override
      public Consumer create() throws Exception
      {
         Properties consumerProperties = new Properties();
         config.applyCommon(consumerProperties);
         return new KafkaConsumer(consumerProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
      }

      @Override
      public PooledObject<Consumer> wrap(Consumer obj)
      {
         return new DefaultPooledObject<>(obj);
      }
   }

}
