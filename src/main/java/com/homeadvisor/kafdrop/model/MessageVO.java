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

package com.homeadvisor.kafdrop.model;

import java.util.Map;
import java.util.TreeMap;

public class MessageVO
{
   private String message;
   private String key;
   private String topic;
   private int partition;
   private long offset;
   private boolean valid = true;
   private long timestamp;
   private String timestampType;
   private final Map<String, String> headers = new TreeMap<>();

   public void addHeader(String key, String value)
   {
      headers.put(key, value);
   }

   public Map<String, String> getHeaders()
   {
      return headers;
   }

   public boolean isValid()
   {
      return valid;
   }

   public void setValid(boolean valid)
   {
      this.valid = valid;
   }

   public String getTopic()
   {
      return topic;
   }

   public void setTopic(String topic)
   {
      this.topic = topic;
   }

   public int getPartition()
   {
      return partition;
   }

   public void setPartition(int partition)
   {
      this.partition = partition;
   }

   public long getOffset()
   {
      return offset;
   }

   public void setOffset(long offset)
   {
      this.offset = offset;
   }

   public String getMessage()
   {
      return message;
   }

   public void setMessage(String message)
   {
      this.message = message;
   }

   public String getKey()
   {
      return key;
   }

   public void setKey(String key)
   {
      this.key = key;
   }

   public long getTimestamp()
   {
      return timestamp;
   }

   public void setTimestamp(long timestamp)
   {
      this.timestamp = timestamp;
   }

   public String getTimestampType()
   {
      return timestampType;
   }

   public void setTimestampType(String timestampType)
   {
      this.timestampType = timestampType;
   }
}
