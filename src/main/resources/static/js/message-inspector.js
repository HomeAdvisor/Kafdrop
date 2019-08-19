/*
 * Copyright 2016 HomeAdvisor, Inc.
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


jQuery(document).ready(function() {
    var isFormLoad
    $(function() {
        isFormLoad = true;
        $('#searchby').trigger('change');
        isFormLoad = false;
    });
    jQuery(document).on('click', '.toggle-msg', function(e) {
        var link = jQuery(this),
            linkIcon = link.find('.fa'),
            body = link.parent().find('.message-body');

        e.preventDefault();

        linkIcon.toggleClass('fa-chevron-circle-right fa-chevron-circle-down')
        if (true == body.data('expanded')) {
            body.text(JSON.stringify(JSON.parse(body.text())));
            body.data('expanded', false);
        } else {
            body.text(JSON.stringify(JSON.parse(body.text()), null, 3));
            body.data('expanded', true);
        }
    });

    jQuery(document).on('change', '#partition', function(e) {
        var selectedOption = jQuery(this).children("option").filter(":selected"),
            firstOffset = selectedOption.data('firstOffset'),
            lastOffset = selectedOption.data('lastOffset');
        jQuery('#firstOffset').text(firstOffset);
        jQuery('#lastOffset').text(lastOffset);
        jQuery('#partitionSize').text(lastOffset - firstOffset)
    });

    jQuery(document).on('change', '#searchby', function(e) {
        var selectedOption = jQuery(this).children("option").filter(":selected").val();
        if (isFormLoad == false) {
            jQuery('#message-display').empty();
        }
        if (selectedOption.toLowerCase() == 'offset') {
            jQuery('#partitionSizes').show();
            jQuery('#offset').show();
            jQuery('#dvMessageKey').hide();
            jQuery('#dvMessageFormat').show();
        } else {
            jQuery('#partitionSizes').hide();
            jQuery('#offset').hide();
            jQuery('#dvMessageKey').show();
            jQuery('#dvMessageFormat').hide();
        }
    });
})