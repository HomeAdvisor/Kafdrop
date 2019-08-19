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

public class MessageVO implements java.io.Serializable
{
   private String message;
   private String key;
   private boolean valid;
   private long checksum;
   private long computedChecksum;
   private String compressionCodec;

   public boolean isValid()
   {
      return valid;
   }

   public void setValid(boolean valid)
   {
      this.valid = valid;
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

   public long getChecksum()
   {
      return checksum;
   }

   public void setChecksum(long checksum)
   {
      this.checksum = checksum;
   }

   public long getComputedChecksum()
   {
      return computedChecksum;
   }

   public void setComputedChecksum(long computedChecksum)
   {
      this.computedChecksum = computedChecksum;
   }

   public String getCompressionCodec()
   {
      return compressionCodec;
   }

   public void setCompressionCodec(String compressionCodec)
   {
      this.compressionCodec = compressionCodec;
   }
}
