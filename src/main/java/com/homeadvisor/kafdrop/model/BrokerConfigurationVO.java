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

package com.homeadvisor.kafdrop.model;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class BrokerConfigurationVO
{
   private final int brokerId;
   private final Map<String, ConfigEntryVO> configEntries = new TreeMap<>();

   public BrokerConfigurationVO(int brokerId, Collection<ConfigEntryVO> configuration)
   {
      this.brokerId = brokerId;
      configuration.forEach(entry -> configEntries.put(entry.getName(), entry));
   }

   public int getBrokerId()
   {
      return brokerId;
   }

   public Collection<ConfigEntryVO> getEntries()
   {
      return configEntries.values();
   }

   public static class ConfigEntryVO
   {
      private final String name;
      private final String value;
      private final String source;
      private final boolean sensitive;

      public ConfigEntryVO(String name, String value, String source, boolean sensitive)
      {
         this.name = name;
         this.value = value;
         this.source = source;
         this.sensitive = sensitive;
      }

      public String getName()
      {
         return name;
      }

      public String getValue()
      {
         return value;
      }

      public String getSource()
      {
         return source;
      }

      public boolean isSensitive()
      {
         return sensitive;
      }
   }
}
