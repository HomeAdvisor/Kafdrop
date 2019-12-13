/*
 * Copyright 2017 HomeAdvisor, Inc.
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

import com.homeadvisor.kafdrop.model.BrokerVO;
import com.homeadvisor.kafdrop.model.BrokerConfigurationVO;
import com.homeadvisor.kafdrop.model.ClusterSummaryVO;
import com.homeadvisor.kafdrop.model.ConsumerVO;
import com.homeadvisor.kafdrop.model.TopicVO;

import java.util.Collection;
import java.util.List;
import java.util.Optional;


public interface KafkaMonitor
{
   List<BrokerVO> getBrokers();

   Optional<BrokerVO> getBroker(int id);

   default Optional<BrokerConfigurationVO> getBrokerConfiguration(int id)
   {
      return Optional.empty();
   }

   List<TopicVO> getTopics();

   Optional<TopicVO> getTopic(String topic);

   ClusterSummaryVO getClusterSummary(Collection<TopicVO> topics);

   List<ConsumerVO> getConsumers(TopicVO topic);

   Optional<ConsumerVO> getConsumer(String groupId);

   Optional<ConsumerVO> getConsumerByTopic(String groupId, TopicVO topic);
}
