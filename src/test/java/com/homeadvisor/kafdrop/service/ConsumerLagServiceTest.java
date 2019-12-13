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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.homeadvisor.kafdrop.model.ConsumerOffsetVO;
import com.homeadvisor.kafdrop.model.ConsumerPartitionVO;
import com.homeadvisor.kafdrop.model.ConsumerTopicVO;
import com.homeadvisor.kafdrop.model.ConsumerVO;
import com.homeadvisor.kafdrop.model.dto.ConsumerDTO;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType.Both;
import static com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType.Kafka;
import static com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType.Minimum;
import static com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType.Zookeeper;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

public class ConsumerLagServiceTest
{

   private final String CONSUMER_GROUP_ID = UUID
         .randomUUID()
         .toString();
   private final String TOPIC0_ID = UUID
         .randomUUID()
         .toString();
   private final String TOPIC1_ID = UUID
         .randomUUID()
         .toString();

   private ConsumerVO TEST_CONSUMER = null;
   private Long expectedKafkaLag;
   private Long expectedZookeeperLag;


   @Before
   public void setup()
   {
      TEST_CONSUMER = new ConsumerVO(CONSUMER_GROUP_ID);

      ConsumerTopicVO topic0 = new ConsumerTopicVO(TOPIC0_ID, CONSUMER_GROUP_ID);
      ConsumerPartitionVO partition0 = new ConsumerPartitionVO(CONSUMER_GROUP_ID, TOPIC0_ID, 0);
      partition0.setSize(randomLong(100, 200));
      ConsumerOffsetVO offset0 = new ConsumerOffsetVO(randomLong(1, 99), randomLong(1, 99));
      partition0.setConsumerOffset(offset0);
      topic0.addOffset(partition0);
      TEST_CONSUMER.addTopic(topic0);

      ConsumerTopicVO topic1 = new ConsumerTopicVO(TOPIC1_ID, CONSUMER_GROUP_ID);
      ConsumerPartitionVO partition1 = new ConsumerPartitionVO(CONSUMER_GROUP_ID, TOPIC1_ID, 1);
      partition1.setSize(randomLong(100, 200));
      ConsumerOffsetVO offset1 = new ConsumerOffsetVO(randomLong(1, 99), randomLong(1, 99));
      partition1.setConsumerOffset(offset1);
      topic1.addOffset(partition1);
      TEST_CONSUMER.addTopic(topic1);

      // compute what we expect to see lag-wise
      long totalMsgs = partition0.getSize() + partition1.getSize();
      expectedKafkaLag = totalMsgs - (offset0.getKafkaOffset() + offset1.getKafkaOffset());
      expectedZookeeperLag = totalMsgs - (offset0.getZookeeperOffset() + offset1.getZookeeperOffset());
   }


   @Test
   public void test_computeConsumerGroupLag_happyPath_both() throws Exception
   {
      CuratorKafkaMonitor mockedMonitor = createMock(CuratorKafkaMonitor.class);
      mockedMonitor.getConsumer(CONSUMER_GROUP_ID);
      expectLastCall().andReturn(Optional.of(TEST_CONSUMER));
      replay(mockedMonitor);
      ConsumerLagService lagService = new ConsumerLagService(mockedMonitor);

      Collection<ConsumerTopicVO> topics = lagService.computeConsumerGroupLag(CONSUMER_GROUP_ID, Both);

      verify(mockedMonitor);
      ConsumerDTO consumerDTO = verifyTestResults(topics, Both, containsString("totalKafkaLag\":" + expectedKafkaLag),
            containsString("totalZookeeperLag\":" + expectedZookeeperLag), not(containsString("totalLag")));
      assertThat(consumerDTO.getTopics(), aMapWithSize(2));
   }

   @Test
   public void test_computeConsumerGroupLag_happyPath_kafka() throws Exception
   {
      CuratorKafkaMonitor mockedMonitor = createMock(CuratorKafkaMonitor.class);
      mockedMonitor.getConsumer(CONSUMER_GROUP_ID);
      expectLastCall().andReturn(Optional.of(TEST_CONSUMER));
      replay(mockedMonitor);
      ConsumerLagService lagService = new ConsumerLagService(mockedMonitor);

      Collection<ConsumerTopicVO> topics = lagService.computeConsumerGroupLag(CONSUMER_GROUP_ID, Kafka);

      verify(mockedMonitor);
      verifyTestResults(topics, Kafka, containsString("totalKafkaLag\":" + expectedKafkaLag), not(containsString
            ("totalZookeeperLag")), not(containsString("totalLag")));
   }

   @Test
   public void test_computeConsumerGroupLag_happyPath_zookeeper() throws Exception
   {
      CuratorKafkaMonitor mockedMonitor = createMock(CuratorKafkaMonitor.class);
      mockedMonitor.getConsumer(CONSUMER_GROUP_ID);
      expectLastCall().andReturn(Optional.of(TEST_CONSUMER));
      replay(mockedMonitor);
      ConsumerLagService lagService = new ConsumerLagService(mockedMonitor);

      Collection<ConsumerTopicVO> topics = lagService.computeConsumerGroupLag(CONSUMER_GROUP_ID, Zookeeper);

      verify(mockedMonitor);
      verifyTestResults(topics, Zookeeper, not(containsString("totalKafkaLag")), containsString("totalZookeeperLag\":" +
            expectedZookeeperLag), not(containsString("totalLag")));
   }

   @Test
   public void test_computeConsumerGroupLag_happyPath_minimum() throws Exception
   {
      CuratorKafkaMonitor mockedMonitor = createMock(CuratorKafkaMonitor.class);
      mockedMonitor.getConsumer(CONSUMER_GROUP_ID);
      expectLastCall().andReturn(Optional.of(TEST_CONSUMER));
      replay(mockedMonitor);
      ConsumerLagService lagService = new ConsumerLagService(mockedMonitor);

      Collection<ConsumerTopicVO> topics = lagService.computeConsumerGroupLag(CONSUMER_GROUP_ID, Minimum);

      verify(mockedMonitor);
      verifyTestResults(topics, Minimum, not(containsString("totalKafkaLag")), not(containsString("totalZookeeperLag")),
            containsString("totalLag\":"));
   }


   private ConsumerDTO verifyTestResults(Collection<ConsumerTopicVO> topics, ConsumerLagReportingOffsetType offsetType,
                                         Matcher<String> stringMatcher, Matcher<String> stringMatcher2, Matcher<String>
                                               totalLag) throws JsonProcessingException
   {
      ConsumerDTO consumerDTO = new ConsumerDTO(topics, offsetType);
      assertThat(consumerDTO, notNullValue());
      assertThat(consumerDTO.getTopics(), aMapWithSize(2));

      ObjectMapper objectMapper = new ObjectMapper();
      String dehydratedDTOs = objectMapper.writeValueAsString(consumerDTO);

      assertThat(dehydratedDTOs, stringMatcher);
      assertThat(dehydratedDTOs, stringMatcher2);
      assertThat(dehydratedDTOs, totalLag);

      return consumerDTO;
   }

   private static long randomLong(int min, int max)
   {
      // nextInt is normally exclusive of the top value, so add 1 to make it inclusive
      return ThreadLocalRandom
            .current()
            .nextLong(min, max + 1);
   }
}
