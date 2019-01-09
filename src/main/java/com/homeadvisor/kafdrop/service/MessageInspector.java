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

import com.homeadvisor.kafdrop.model.MessageVO;
import com.homeadvisor.kafdrop.model.TopicPartitionVO;
import com.homeadvisor.kafdrop.model.TopicVO;
import com.homeadvisor.kafdrop.model.BrokerVO;
import com.homeadvisor.kafdrop.util.BrokerChannel;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import scala.collection.parallel.ParIterableLike.Take;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Console;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/* r&d */
// import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.log4j.Level;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.json4s.*;
import org.json4s.jackson.JsonMethods;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;




@Service
public class MessageInspector
{
   private final Logger LOG = LoggerFactory.getLogger(getClass());

   @Autowired
   private KafkaMonitor kafkaMonitor;

   public List<MessageVO> getMessages(String topicName, int partitionId, long offset, long count)
   {
      final TopicVO topic = kafkaMonitor.getTopic(topicName).orElseThrow(TopicNotFoundException::new);
      final TopicPartitionVO partition = topic.getPartition(partitionId).orElseThrow(PartitionNotFoundException::new);

      return kafkaMonitor.getBroker(partition.getLeader().getId())
         .map(broker -> {
            SimpleConsumer consumer = new SimpleConsumer(broker.getHost(), broker.getPort(), 10000, 100000, "");

            final FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder()
               .clientId("KafDrop")
               .maxWait(5000) // todo: make configurable
               .minBytes(1);

            List<MessageVO> messages = new ArrayList<>();
            long currentOffset = offset;
            while (messages.size() < count)
            {
               final FetchRequest fetchRequest =
                  fetchRequestBuilder
                     .addFetch(topicName, partitionId, currentOffset, 1024 * 1024)
                     .build();

               FetchResponse fetchResponse = consumer.fetch(fetchRequest);

               final ByteBufferMessageSet messageSet = fetchResponse.messageSet(topicName, partitionId);
               if (messageSet.validBytes() <= 0) break;


               int oldSize = messages.size();
               StreamSupport.stream(messageSet.spliterator(), false)
                  .limit(count - messages.size())
                  .map(MessageAndOffset::message)
                  .map(this::createMessage)
                  .forEach(messages::add);
               currentOffset += messages.size() - oldSize;
            }
            return messages;
         })
         .orElseGet(Collections::emptyList);
   }

   public List<MessageVO> getMessagesByKey(String topicName, String messageKey, int partitionId, long numberOfMessages) // New fn added by Tamalendu Nath
   {
      final TopicVO topic = kafkaMonitor.getTopic(topicName).orElseThrow(TopicNotFoundException::new);
      // final TopicPartitionVO partition = topic.getPartition(partitionId).orElseThrow(PartitionNotFoundException::new);
      Collection<TopicPartitionVO> partitions = topic.getPartitions();
      List<BrokerVO> broker = kafkaMonitor.getBrokers();

      String brokerList = "";
      for (BrokerVO aBroker : broker) {
         if (brokerList.length()>0) {
            brokerList += ",";
         }
         brokerList += aBroker.getHost() + ":" + aBroker.getPort();
      }
      String consumerName = "kafka-test-ARRTP";

      Map<String, Object> param = new HashMap<String, Object>();
      param.put("bootstrap.servers", brokerList);
      param.put("key.deserializer", StringDeserializer.class);
      param.put("value.deserializer", StringDeserializer.class);
      param.put("group.id", consumerName);
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
      SparkSession spark = SparkSession.builder().appName(consumerName).master("local[*]").getOrCreate();
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
            // System.out.println(r);
            if (r != null) {
               String messageKey2 = messageKey;
            return r.getKey().equals(messageKey2);
            }
            return false;
         }).collect();

      // Dataset<Row> ds = spark.sqlContext().createDataFrame(newRdd, MessageVO.class);


            
      jsc.stop();
      List<MessageVO> messages = new ArrayList<>();
      for (MessageVO aMessage : newRdd) {
         messages.add(aMessage);
      }
      
      return messages;
   }
   
   // public List<MessageVO> getMessagesByKey(String topicName, String messageKey, int partitionId) // New fn added by Tamalendu Nath
   // {
   //    final TopicVO topic = kafkaMonitor.getTopic(topicName).orElseThrow(TopicNotFoundException::new);
   //    final TopicPartitionVO partition = topic.getPartition(partitionId).orElseThrow(PartitionNotFoundException::new);
   //    Collection<TopicPartitionVO> partitions = topic.getPartitions();
   //    List<BrokerVO> broker = kafkaMonitor.getBrokers();

   //    String brokerList = "";
   //    for (BrokerVO aBroker : broker) {
   //       if (brokerList.length()>0) {
   //          brokerList += ",";
   //       }
   //       brokerList += aBroker.getHost() + ":" + aBroker.getPort();
   //    }
   //    String consumerName = "kafka-test-ARRTP";

   //    Set<String> topics =new HashSet<String>( Arrays.asList( topic.getName()));
   //    Map<String, Object> param = new HashMap<String, Object>();
   //    param.put("bootstrap.servers", brokerList);
   //    param.put("key.deserializer", StringDeserializer.class);
   //    param.put("value.deserializer", StringDeserializer.class);
   //    param.put("group.id", consumerName);
   //    param.put("auto.offset.reset", "earliest");
   //    param.put("enable.auto.commit", false);

   //    // KafkaConsumer consumer = new KafkaConsumer<>(param);
   //    OffsetRange[] offsetRanges = new OffsetRange[partitions.size()];
   //    for (TopicPartitionVO aPart : partitions) {
   //       offsetRanges[aPart.getId()] = OffsetRange.create(topic.getName(),aPart.getId(),aPart.getFirstOffset(),aPart.getSize());
   //    }
   //    SparkSession spark = SparkSession.builder().appName(consumerName).master("local[*]").getOrCreate();
   //    JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(spark.sparkContext()), Durations.seconds(45));
   //    JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream =
   //              KafkaUtils.createDirectStream(
   //                jssc,
   //                  LocationStrategies.PreferConsistent(),
   //                  ConsumerStrategies.<String, String>Subscribe(topics, param));
   //    System.out.println(messageKey);
   //    directKafkaStream.foreachRDD(rdd -> {
   //       System.out.println("--- New RDD with " + rdd.partitions().size()+ " partitions and " + rdd.count() + " records");
   //       rdd.map(
   //       (Function<ConsumerRecord<String, String>, MessageVO>) r -> {
   //          MessageVO vo = new MessageVO();
   //          vo.setMessage(r.value());
   //          vo.setKey(r.key());
   //          vo.setComputedChecksum(r.checksum());
   //          vo.setChecksum(r.checksum());
   //          vo.setCompressionCodec("none");
   //          return vo;
   //          }).filter( r -> {
   //             // System.out.println(r);
   //             if (r != null) {
   //                String messageKey2 = messageKey;
   //             return r.getKey().equals(messageKey2);
   //             }
   //             return false;
   //          }).foreach(r -> {
   //             System.out.println(r.getMessage());
   //             System.out.println(System.currentTimeMillis());
   //          });

   //   });
   //   System.out.println(System.currentTimeMillis());
   //    jssc.start();
   //    List<MessageVO> messages = new ArrayList<>();
   //    // for (MessageVO aMessage : newRdd) {
   //    //    messages.add(aMessage);
   //    // }
      
   //    return messages;
   // }

   private MessageVO createMessage(Message message)
   {
      MessageVO vo = new MessageVO();
      if (message.hasKey())
      {
         vo.setKey(readString(message.key()));
      }
      if (!message.isNull())
      {
         vo.setMessage(readString(message.payload()));
      }

      vo.setValid(message.isValid());
      vo.setCompressionCodec(message.compressionCodec().name());
      vo.setChecksum(message.checksum());
      vo.setComputedChecksum(message.computeChecksum());

      return vo;
   }

   private String readString(ByteBuffer buffer)
   {
      try
      {
         return new String(readBytes(buffer), "UTF-8");
      }
      catch (UnsupportedEncodingException e)
      {
         return "<unsupported encoding>";
      }
   }

   private byte[] readBytes(ByteBuffer buffer)
   {
      return readBytes(buffer, 0, buffer.limit());
   }

   private byte[] readBytes(ByteBuffer buffer, int offset, int size)
   {
      byte[] dest = new byte[size];
      if (buffer.hasArray())
      {
         System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, size);
      }
      else
      {
         buffer.mark();
         buffer.get(dest);
         buffer.reset();
      }
      return dest;
   }

}
