<#--
 Copyright 2016 HomeAdvisor, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<#import "lib/template.ftl" as template>
<#import "/spring.ftl" as spring />
<@template.header "Topic: ${topic.name}: Messages">
   <style type="text/css">
       h1 { margin-bottom: 16px; }
       #messageFormPanel { margin-top: 16px; }
       #partitionSizes { margin-left: 16px; }
       .toggle-msg { float: left;}
       .toggle-header { float: left;}
       .message-headers { display: none; }
       .header-key { margin-right: 6px; }
       .message-wrapper { margin-top: 6px;}
   </style>

  <script src="/js/message-inspector.js"></script>
</@template.header>
<#setting number_format="0">


<h1>Topic Messages: <a href="/topic/${topic.name}">${topic.name}</a></h1>

<#assign selectedPartition=messageForm.partition!0?number>
<#assign selectedFormat=messageForm.format!defaultFormat>

<div id="partitionSizes">
    <#assign curPartition=topic.getPartition(selectedPartition).get()>
    <span class="label label-default">First Offset:</span> <span id="firstOffset">${curPartition.firstOffset}</span>
    <span class="label label-default">Last Offset:</span> <span id="lastOffset">${curPartition.size}</span>
    <span class="label label-default">Size:</span> <span id="partitionSize">${curPartition.size - curPartition.firstOffset}</span>
</div>

<div id="messageFormPanel" class="panel panel-default">
<form method="GET" action="/topic/${topic.name}/messages" id="messageForm" class="form-inline panel-body">

    <div class="form-group">
        <label for="partition">Partition</label>
        <select id="partition" name="partition">
        <#list topic.partitions as p>
            <option value="${p.id}" data-first-offset="${p.firstOffset}" data-last-offset="${p.size}" <#if p.id == selectedPartition>selected="selected"</#if>>${p.id}</option>
        </#list>
        </select>
    </div>

    <@spring.bind path="messageForm.offset"/>
    <div class="form-group ${spring.status.error?string("has-error", "")}">
        <label class="control-label" for="offset">Offset</label>
        <@spring.formInput path="messageForm.offset" attributes='class="form-control"'/>
        <#if spring.status.error>
            <span class="text-danger"><i class="fa fa-times-circle"></i><@spring.showErrors "<br/>"/></span>
        </#if>
    </div>

    <@spring.bind path="messageForm.count"/>
    <div class="form-group ${spring.status.error?string("has-error", "")}">
        <label class=control-label" for="count">Num Messages</label>
        <@spring.formInput path="messageForm.count" attributes='class="form-control ${spring.status.error?string("has-error", "")}"'/>
        <#if spring.status.error>
           <span class="text-danger"><i class="fa fa-times-circle"></i><@spring.showErrors "<br/>"/></span>
        </#if>
    </div>

    <div class="form-group">
        <label for="format">Message Format</label>
        <select id="format" name="format">
        <#list messageFormats as f>
            <option value="${f}"<#if f == selectedFormat>selected="selected"</#if>>${f}</option>
        </#list>
        </select>
    </div>

    <button class="btn btn-primary" type="submit"><i class="fa fa-search"></i> View Messages</button>
    
</form>
</div>

<@spring.bind path="messageForm.*"/>
<div id="message-display" class="container">
    <#if messages?? && messages?size gt 0>
    <#list messages as msg>
        <#assign offset=messageForm.offset + msg_index>
        <div data-offset="${offset}" class="message-detail">
            <span class="label label-default">Offset:</span> ${msg.offset}
            <span class="label label-default">Key:</span> ${msg.key!''}

            <span class="label label-default">Timestamp:</span>
            <#if msg.timestampType = 'NoTimestampType'>
                None
            <#else>
                ${msg.timestamp} (${msg.timestampType!'Unkown'})
            </#if>
            <#if msg.headers?size gt 0>
                <div class="headers-wrapper">
                    <a href="#" class="toggle-header"><i class="fa fa-plus-circle">&nbsp;</i></a>
                    <div class="message-headers-label">
                        Headers
                    </div>
                    <div class="message-headers">
                        <table>
                        <#list msg.headers?keys as key>
                            <tr>
                                <td class="header-key label label-info">${key}:</td>
                                <td class="header-value">${msg.headers[key]}</td>
                            </tr>
                        </#list>
                        </table>
                    </div>
                </div>
            </#if>
            <div class="message-wrapper">
            <a href="#" class="toggle-msg"><i class="fa fa-chevron-circle-right">&nbsp;</i></a>
            <pre class="message-body"><xmp>${msg.message!''}</xmp></pre>
            </div>
        </div>
    </#list>
    <#elseif !(spring.status.error) && !(messageForm.empty)>
        No messages found in partition ${(messageForm.partition)!"PARTITION_NOT_SET"} at offset ${messageForm.offset}
    </#if>
</div>

<@template.footer/>
