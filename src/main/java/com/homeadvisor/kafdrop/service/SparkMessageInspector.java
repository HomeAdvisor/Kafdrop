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

import com.homeadvisor.kafdrop.config.MessageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;

import com.homeadvisor.kafdrop.model.MessageVO;
import com.homeadvisor.kafdrop.model.TopicPartitionVO;
import com.homeadvisor.kafdrop.model.TopicVO;
import com.homeadvisor.kafdrop.model.BrokerVO;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;


import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;


@Service
public class SparkMessageInspector
{
   private final Logger LOG = LoggerFactory.getLogger(getClass());

   @Autowired
   private KafkaMonitor kafkaMonitor;

   @Autowired
   private MessageConfiguration.MessageProperties messageProperties;

   public List<MessageVO> getMessagesByKey(String topicName, String messageKey, long numberOfMessages) 
   {
      final TopicVO topic = kafkaMonitor.getTopic(topicName).orElseThrow(TopicNotFoundException::new);
      Collection<TopicPartitionVO> partitions = topic.getPartitions();
      List<BrokerVO> broker = kafkaMonitor.getBrokers();

      String brokerList = "";
      for (BrokerVO aBroker : broker) {
         if (brokerList.length()>0) {
            brokerList += ",";
         }
         brokerList += aBroker.getHost() + ":" + aBroker.getPort();
      }
      
      Map<String, Object> param = new HashMap<String, Object>();
      param.put("bootstrap.servers", brokerList);
      param.put("key.deserializer", StringDeserializer.class);
      param.put("value.deserializer", StringDeserializer.class);
      param.put("group.id", messageProperties.getConsumerName());
      param.put("auto.offset.reset", "earliest");
      param.put("enable.auto.commit", false);

      OffsetRange[] offsetRanges = new OffsetRange[partitions.size()];
      for (TopicPartitionVO aPart : partitions) {
         long startOffset = aPart.getSize() - numberOfMessages;
         if (startOffset <= aPart.getFirstOffset()) {
            startOffset = aPart.getFirstOffset();
         }
         offsetRanges[aPart.getId()] = OffsetRange.create(topic.getName(),aPart.getId(),startOffset,aPart.getSize());
      }
      SparkSession spark = SparkSession.builder().appName(messageProperties.getConsumerName()).master("local[*]").getOrCreate();
      SparkContext sc =  spark.sparkContext();
      JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
      JavaRDD<ConsumerRecord<String, String>> rdd =KafkaUtils.createRDD(jsc, param, offsetRanges, LocationStrategies.PreferConsistent());
      List<MessageVO> newRdd  = rdd.map(
      (Function<ConsumerRecord<String, String>, MessageVO>) r -> {
         MessageVO vo = new MessageVO();
         vo.setMessage(r.value());
         vo.setKey(r.key());
         vo.setComputedChecksum(r.checksum());
         vo.setChecksum(r.checksum());
         vo.setCompressionCodec("none");
         return vo;
         }).filter( r -> {
            if (r != null) {
               String messageKey2 = messageKey;
            return r.getKey().equals(messageKey2);
            }
            return false;
         }).collect();

      jsc.stop();
      List<MessageVO> messages = new ArrayList<>();
      for (MessageVO aMessage : newRdd) {
         messages.add(aMessage);
      }
      return messages;
   }
}
