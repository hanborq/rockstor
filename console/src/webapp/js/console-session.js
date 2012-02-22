/*
 * Copyright 2012 Hanborq Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var user;
var site;
var publicSite;
var authProtocal;

function loadUserInfo() {
  $("#user_name").html("loding user info ...");
  $.ajax({
      url: '/oper/UserInfo.action',
      type: 'POST',
      data: '',
      success: function(m){
		  user=m.user;
		  $("#user_name").html("hi " + user.dispname);
          loadSiteInfo();
          startPoll();
      },
      error: function(){
          alert('获取用户信息失败');
      }
  });
}

function loadSiteInfo() {
  $.ajax({
      url: '/oper/SiteInfo.action',
      type: 'POST',
      data: '',
      success: function(m){
		  site=m.site;
		  publicSite=m.publicSite;
		  authProtocal=m.authProtocal;
          getService();
          loadBucketContextMenu2();
      },
      error: function(){
          alert('Get Site info Error');
      }
  });
}

function startPoll() {
  $("body").everyTime('180s',function(){
    $.ajax({
      url: '/oper/Session.action',
      type: 'POST',
      data: '',
      async: false,
      success: function(m){
      },
      error: function(){
      }
    });
  });
}
