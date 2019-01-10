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

package com.homeadvisor.kafdrop.controller;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.homeadvisor.kafdrop.config.MessageFormatConfiguration;
import com.homeadvisor.kafdrop.config.SchemaRegistryConfiguration;
import com.homeadvisor.kafdrop.config.MessageConfiguration;
import com.homeadvisor.kafdrop.model.MessageVO;
import com.homeadvisor.kafdrop.model.TopicVO;
import com.homeadvisor.kafdrop.service.KafkaMonitor;
import com.homeadvisor.kafdrop.service.MessageInspector;
import com.homeadvisor.kafdrop.service.SparkMessageInspector;
import com.homeadvisor.kafdrop.service.TopicNotFoundException;
import com.homeadvisor.kafdrop.util.AvroMessageDeserializer;
import com.homeadvisor.kafdrop.util.DefaultMessageDeserializer;
import com.homeadvisor.kafdrop.util.MessageDeserializer;
import com.homeadvisor.kafdrop.util.MessageFormat;
import com.homeadvisor.kafdrop.util.SearchType;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

@Controller
public class MessageController
{
   @Autowired
   private KafkaMonitor kafkaMonitor;

   @Autowired
   private MessageInspector messageInspector;

   @Autowired
   private SparkMessageInspector sparkMessageInspector;

   @Autowired
   private MessageFormatConfiguration.MessageFormatProperties messageFormatProperties;

   @Autowired
   private SchemaRegistryConfiguration.SchemaRegistryProperties schemaRegistryProperties;

   @Autowired
   private MessageConfiguration.MessageProperties searchTypeProperties;

   /**
    * Human friendly view of reading messages.
    * @param topicName Name of topic
    * @param messageForm Message form for submitting requests to view messages.
    * @param errors
    * @param model
    * @return View for seeing messages in a partition.
    */
   @RequestMapping(method = RequestMethod.GET, value = "/topic/{name:.+}/messages")
   public String viewMessageForm(@PathVariable("name") String topicName,
                                 @Valid @ModelAttribute("messageForm") PartitionOffsetInfo messageForm,
                                 BindingResult errors,
                                 Model model)
   {
      final MessageFormat defaultFormat = messageFormatProperties.getFormat();
      final SearchType defaultSearchType = searchTypeProperties.getType();
      if (messageForm.isEmpty())
      {
         final PartitionOffsetInfo defaultForm = new PartitionOffsetInfo();

         defaultForm.setCount(10l);
         defaultForm.setOffset(0l);
         defaultForm.setPartition(0);
         defaultForm.setFormat(defaultFormat);

         model.addAttribute("messageForm", defaultForm);
      }

      final TopicVO topic = kafkaMonitor.getTopic(topicName)
         .orElseThrow(() -> new TopicNotFoundException(topicName));
      model.addAttribute("topic", topic);

      model.addAttribute("defaultFormat", defaultFormat);
      model.addAttribute("messageFormats", MessageFormat.values());
      model.addAttribute("defaultSearchType", defaultSearchType);
      model.addAttribute("searchType", SearchType.values());
      
      if (messageForm.getSearchby() != null && messageForm.getSearchby().name() == SearchType.Key.toString()) {
         if (!messageForm.isEmptyMessageKey() && !errors.hasErrors())
         {
            model.addAttribute("messages",
               sparkMessageInspector.getMessagesByKey(topicName,
                                                            messageForm.getMessageKey(),
                                                            messageForm.getCount()));
         }
      } else {
         if (!messageForm.isEmpty() && !errors.hasErrors())
         {
            final MessageDeserializer deserializer = getDeserializer(
                  topicName, messageForm.getFormat());
   
            model.addAttribute("messages",
                               messageInspector.getMessages(topicName,
                                                            messageForm.getPartition(),
                                                            messageForm.getOffset(),
                                                            messageForm.getCount(),
                                                            deserializer));
   
         }
      }
      return "message-inspector";
   }

   /**
    * Return a JSON list of all partition offset info for the given topic. If specific partition
    * and offset parameters are given, then this returns actual kafka messages from that partition
    * (if the offsets are valid; if invalid offsets are passed then the message list is empty).
    * @param topicName Name of topic.
    * @return Offset or message data.
    */
   @ApiOperation(value = "getPartitionOrMessages", notes = "Get offset or message data for a topic. Without query params returns all partitions with offset data. With query params, returns actual messages (if valid offsets are provided).")
   @ApiResponses(value = {
         @ApiResponse(code = 200, message = "Success", response = List.class),
         @ApiResponse(code = 404, message = "Invalid topic name")
   })
   @RequestMapping(method = RequestMethod.GET, value = "/topic/{name:.+}/messages", produces = MediaType.APPLICATION_JSON_VALUE)
   public @ResponseBody List<Object> getPartitionOrMessages(
         @PathVariable("name") String topicName,
         @RequestParam(name = "partition", required = false) Integer partition,
         @RequestParam(name = "offset", required = false)    Long offset,
         @RequestParam(name = "count", required = false)     Long count
   )
   {
      if(partition == null || offset == null || count == null)
      {
         final TopicVO topic = kafkaMonitor.getTopic(topicName)
               .orElseThrow(() -> new TopicNotFoundException(topicName));

         List<Object> partitionList = new ArrayList<>();
         topic.getPartitions().stream().forEach(vo -> partitionList.add(new PartitionOffsetInfo(vo.getId(), vo.getFirstOffset(), vo.getSize())));

         return partitionList;
      }
      else
      {
         // Currently, only default deserialization supported via JSON API.
         final MessageDeserializer deserializer = new DefaultMessageDeserializer();

         List<Object> messages = new ArrayList<>();
         List<MessageVO> vos = messageInspector.getMessages(
               topicName,
               partition,
               offset,
               count,
               deserializer);

         if(vos != null)
         {
            vos.stream().forEach(vo -> messages.add(vo));
         }

         return messages;
      }
   }

   private MessageDeserializer getDeserializer(String topicName, MessageFormat format) {
      final MessageDeserializer deserializer;

      if (format == MessageFormat.AVRO) {
         final String schemaRegistryUrl = schemaRegistryProperties.getConnect();
         deserializer = new AvroMessageDeserializer(topicName, schemaRegistryUrl);
      } else {
         deserializer = new DefaultMessageDeserializer();
      }

      return deserializer;
   }

   /**
    * Encapsulates offset data for a single partition.
    */
   public static class PartitionOffsetInfo
   {
      @NotNull
      @Min(0)
      private Integer partition;

      /**
       * Need to clean this up. We're re-using this form for the JSON message API
       * and it's a bit confusing to have the Java variable and JSON field named
       * differently.
       */
      @NotNull
      @Min(0)
      @JsonProperty("firstOffset")
      private Long offset;

      /**
       * Need to clean this up. We're re-using this form for the JSON message API
       * and it's a bit confusing to have the Java variable and JSON field named
       * differently.
       */
      @NotNull
      @Min(1)
      @JsonProperty("lastOffset")
      private Long count;

      private MessageFormat format;
      private SearchType searchby;
      private String messageKey;
      public PartitionOffsetInfo(int partition, long offset, long count, MessageFormat format, SearchType searchby)
      {
         this.partition = partition;
         this.offset = offset;
         this.count = count;
         this.format = format;
         this.searchby = searchby;
      }

      public PartitionOffsetInfo(int partition, long offset, long count)
      {
         this(partition, offset, count, MessageFormat.DEFAULT, SearchType.Offset);
      }

      public PartitionOffsetInfo()
      {

      }

      @JsonIgnore
      public boolean isEmpty()
      {
         return partition == null && offset == null && (count == null || count == 1);
      }
      public boolean isEmptyMessageKey()
      {
         return messageKey == null && (count == null || count == 0);
      }
      public Integer getPartition()
      {
         return partition;
      }

      public void setPartition(Integer partition)
      {
         this.partition = partition;
      }

      public Long getOffset()
      {
         return offset;
      }

      public void setOffset(Long offset)
      {
         this.offset = offset;
      }

      public Long getCount()
      {
         return count;
      }

      public void setCount(Long count)
      {
         this.count = count;
      }

      public MessageFormat getFormat()
      {
         return format;
      }

      public void setFormat(MessageFormat format)
      {
         this.format = format;
      }
      public String getMessageKey() 
      {
         return this.messageKey;
      }
      public void setMessageKey(String messageKey) 
      {
         this.messageKey = messageKey;
      }

      public SearchType getSearchby()
      {
         return searchby;
      }

      public void setSearchby(SearchType searchby)
      {
         this.searchby = searchby;
      }
   }
}
