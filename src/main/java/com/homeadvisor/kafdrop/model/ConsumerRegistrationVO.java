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
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class ConsumerRegistrationVO
{
   private String id;
   private Map<String, Integer> subscriptions;

   public ConsumerRegistrationVO(String id)
   {
      this.id = id;
   }

   public String getId()
   {
      return id;
   }

   public Map<String, Integer> getSubscriptions()
   {
      return subscriptions;
   }

   public void setSubscriptions(Map<String, Integer> subscriptions)
   {
      this.subscriptions = subscriptions;
   }

   public boolean subscribesToTopic(String topic)
   {
      return subscriptions.keySet()
         .stream().anyMatch(sub -> topicMatchesSubscription(topic, sub));
   }

   private boolean topicMatchesSubscription(String topic, String subscription)
   {
      try
      {
         return (topic.equals(subscription) || topic.matches(subscription));
      }
      catch (PatternSyntaxException ex)
      {
         return false;
      }
   }
}
