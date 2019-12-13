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

import com.homeadvisor.kafdrop.config.KafkaConfiguration;
import com.homeadvisor.kafdrop.model.MessageVO;
import com.homeadvisor.kafdrop.util.MessageDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

import static com.homeadvisor.kafdrop.util.ByteUtils.readString;


@Service
public class MessageInspector
{
   private final Logger LOG = LoggerFactory.getLogger(getClass());

   private KafkaConfiguration config;

   public MessageInspector(KafkaConfiguration config)
   {
      this.config = config;
   }

   private Consumer<byte[], byte[]> createConsumer()
   {
      Properties properties = new Properties();
      config.applyCommon(properties);
      return new KafkaConsumer(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());

   }

   public List<MessageVO> getMessages(
           String topicName,
           int partitionId,
           long offset,
           long count,
           MessageDeserializer deserializer)
   {
      try (Consumer<byte[], byte[]> consumer = createConsumer())
      {
         TopicPartition topicPartition = new TopicPartition(topicName, partitionId);
         consumer.assign(Collections.singletonList(topicPartition));
         consumer.seek(topicPartition, offset);

         // Need to adjust how many records we read based on how many are actually available in the partition.
         long maxOffset = consumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);

         int numRecordsToRead = (int) Math.min(count, maxOffset - offset);

         List<MessageVO> recordList = new ArrayList<>(numRecordsToRead);
         while (recordList.size() < numRecordsToRead)
         {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000L));
            List<ConsumerRecord<byte[], byte[]>> returnedRecords = records.records(topicPartition);

            returnedRecords.subList(0, Math.min((int) count - recordList.size(), returnedRecords.size()))
               .stream()
               .map(record -> createMessage(record, deserializer))
               .forEach(recordList::add);
         }

         return recordList;
      }
   }
   private MessageVO createMessage(ConsumerRecord<byte[], byte[]> record, MessageDeserializer deserializer)
   {
      MessageVO vo = new MessageVO();
      vo.setTopic(record.topic());
      vo.setOffset(record.offset());
      vo.setPartition(record.partition());
      if (record.key() != null && record.key().length > 0)
      {
         vo.setKey(readString(record.key()));
      }
      if (record.value() != null && record.value().length > 0)
      {
         final String messageString;
         if (deserializer != null)
         {
            messageString = deserializer.deserializeMessage(ByteBuffer.wrap(record.value()));
         }
         else
         {
            messageString = readString(record.value());
         }
         vo.setMessage(messageString);
      }

      vo.setTimestamp(record.timestamp());
      vo.setTimestampType(record.timestampType().toString());

      StreamSupport.stream(record.headers().spliterator(), false)
         .forEachOrdered(header -> vo.addHeader(header.key(), new String(header.value(), StandardCharsets.UTF_8)));

      return vo;
   }


}
