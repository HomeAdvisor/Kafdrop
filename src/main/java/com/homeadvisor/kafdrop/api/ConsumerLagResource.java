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

package com.homeadvisor.kafdrop.api;

import com.homeadvisor.kafdrop.model.ConsumerTopicVO;
import com.homeadvisor.kafdrop.model.dto.ConsumerDTO;
import com.homeadvisor.kafdrop.service.ConsumerLagReportingOffsetType;
import com.homeadvisor.kafdrop.service.ConsumerLagService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;


@RestController
public class ConsumerLagResource
{
   @Autowired
   private ConsumerLagService consumerLagService;


   @ApiOperation(value = "getConsumerGroupLag", notes = "Get lag details for a consumer group (all topics, all partitions)")
   @ApiResponses(value = {
         @ApiResponse(code = 200, message = "Success", response = ConsumerDTO.class),
         @ApiResponse(code = 404, message = "Invalid consumer group")
   })
   @RequestMapping(path = "/api/consumer/{groupId}/lag", produces = MediaType.APPLICATION_JSON_VALUE, method = RequestMethod.GET)
   public @ResponseBody
   ConsumerDTO getConsumerGroupLag(@PathVariable("groupId") String groupId, @RequestParam(name =
         "source", required = false, defaultValue = "minimum") String source)
   {
      ConsumerLagReportingOffsetType offsetType = ConsumerLagReportingOffsetType.enumValue(source);
      Collection<ConsumerTopicVO> topics = consumerLagService.computeConsumerGroupLag(groupId, ConsumerLagReportingOffsetType
         .enumValue(source));
      return new ConsumerDTO(topics, offsetType);

   }

   @ApiOperation(value = "getConsumerGroupLagForTopic", notes = "Get lag details for a consumer group topic (all partitions)")
   @ApiResponses(value = {
         @ApiResponse(code = 200, message = "Success", response = ConsumerDTO.class),
         @ApiResponse(code = 404, message = "Invalid consumer group or topic name")
   })
   @RequestMapping(path = "/api/consumer/{groupId}/lag/{topicId:.+}", produces = MediaType.APPLICATION_JSON_VALUE, method =
         RequestMethod
         .GET)
   public @ResponseBody
   ConsumerDTO getConsumerGroupLagForTopic(@PathVariable("groupId") String groupId, @PathVariable("topicId")
         String topicId, @RequestParam(name = "source", required = false, defaultValue = "minimum") String source)
   {
      ConsumerLagReportingOffsetType offsetType = ConsumerLagReportingOffsetType.enumValue(source);
      Collection<ConsumerTopicVO> topics = consumerLagService.computeConsumerGroupLag(groupId, offsetType, topicId);
      return new ConsumerDTO(topics, offsetType);
   }
}
