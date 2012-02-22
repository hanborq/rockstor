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

var bucketList=null;
var objectList=null;
var currentAclList=null;
var currentAclTarget=null;

function buildTimeString(){
  var d=new Date();
  var t=d.getFullYear()+"-"+((d.getMonth()+1)<10?("0"+(d.getMonth()+1)):(d.getMonth()+1))+"-"+(d.getDate()<10?("0"+d.getDate()):d.getDate())+"T"+(d.getHours()<10?("0"+d.getHours()):d.getHours())+":"+(d.getMinutes()<10?("0"+d.getMinutes()):d.getMinutes())+":"+(d.getSeconds()<10?("0"+d.getSeconds()):d.getSeconds())+"."+(d.getMilliseconds()<10?("00"+d.getMilliseconds()):(d.getMilliseconds()<100?("0"+d.getMilliseconds()):d.getMilliseconds()))+"+0800";
  return t;  
}

function buildAuthorizationString(method,date){
  var sig=method+date+user.consoleSecurityKey
  var signature = hex_md5(sig);
  var authorizationString = authProtocal+" "+user.consoleAccessKey+":"+signature;
  return authorizationString;
}

function buildRequest(method, resource, head, body){
  var date = buildTimeString();
  var authorization = buildAuthorizationString(method,date);
  var req={
      "method":method,
      "url":site+resource,
      "head":{
             "Date":date,
             "Content-Length":"0",
             "Authorization":authorization
             }
	  };
  if (head!=null)
    for (var v in head)
      req.head[v]=head[v]
  if (body!=null)
    req.body=body;

  var reqString = $.toJSON(req);
  return encodeURIComponent(reqString);
}

function normalizeObjectName(objName) {
  if (objName=="")
    return objName;
  var paths = objName.split("/");
  var path ="";
  if (paths != null)
    for (var i=0; i<paths.length; i++) {
      if (paths[i] != "")
        path+=(encodeURIComponent(paths[i])+"/");
    }
  if (objName.lastIndexOf("/")==(objName.length-1)) {
    return path;
  } else {
    return path.substring(0, (path.length-1));
  }
}

function getService(){
  $("#bucket_container").html("Loading Service Info ...");
  bucketList=null;
  var reqString = buildRequest("GET","");
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
        $("#bucket_container").html("Loading Service Info Error : "+m.httpResp.body);
		return;
	  }
      var xmlData=xml2json.parser(m.httpResp.body);
	  if (m.httpResp.status!="200"){
        var error=xmlData.error;
        $("#bucket_container").html("Loading Service Info Error : <br>Code : "+error.code+"<br>Message : "+error.messgae);
		return;
      }
      bucketList = xmlData.listallmybucketsresult;
      if (typeof bucketList.buckets.bucket == "undefined") {
        bucketList.buckets.bucket = new Array();
	  } else if (!(bucketList.buckets.bucket instanceof Array)) {
        var tmp = bucketList.buckets.bucket;
        bucketList.buckets.bucket = new Array();
        bucketList.buckets.bucket[0] = tmp;
      }
	  
      var tmpbucketVFolder = new Array();
      for (var i=0; i<bucketList.buckets.bucket.length; i++) {
        var old = null;
        for (var j=0; j<bucketVFolder.length ;j++) {
          if (bucketVFolder[j].name==bucketList.buckets.bucket[i].name) {
            old = bucketVFolder[j];
            break;
          }
        }
        if (old!=null) {
          tmpbucketVFolder[i] = old;
		} else {
          tmpbucketVFolder[i] = {
              name : bucketList.buckets.bucket[i].name,
              vFolder : new VFolder(""),
              };
        }
      }
	  bucketVFolder = tmpbucketVFolder;
      loadBucketView();
    },
    error: function(){
      $("#bucket_container").html("Loading Service Info Error.");
    }
  });
}

function putBucket(){
  var bucket = $("#new_bucket_name")[0].value;
  var acls = document.getElementsByName("new_bucket_set_acl");
  var acl;
  for (var i=0; i<acls.length; i++){
    if (acls[i].checked) {
      $("#new_bucket_message_div").html("Creating bucket : "+bucket+" "+acls[i].value);
      acl=acls[i].value;
    }
  }
  var head= new Object();
  head["rockstor-acl"]=acl;
  var reqString = buildRequest("PUT",bucket,head);
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    data: 'httpReq='+reqString,
    success: function(m){
      $("#new_bucket_set_name_button").button("enable");
      $("#new_bucket_set_acl_button").button("enable");
      $("#new_bucket_create_button").button("enable");
      $("#new_bucket_cancel_button").button("enable");
      if (m.httpResp.status=="600"){
        $("#new_bucket_message_div").html("Create New Bucket Error : "+m.httpResp.body);
        addActionItem("CreateBucket", "Create Bucket "+$("#new_bucket_name")[0].value, "Fail");
		return;
	  }
	  if (m.httpResp.status!="200"){
        var xmlData=xml2json.parser(m.httpResp.body);
        var error=xmlData.error;
        $("#new_bucket_message_div").html("Create New Bucket Error : "+error.code+" : Message : "+error.messgae);
		addActionItem("CreateBucket", "Create Bucket "+$("#new_bucket_name")[0].value, "Fail");
		return;
      }

      $("#new_bucket_message_div").html("Create New Bucket OK.");
      getService();
      addActionItem("CreateBucket", "Create Bucket "+$("#new_bucket_name")[0].value, "Success");
      $("#create_bucket_div").dialog( "close" );
    },
    error: function(){
      $("#new_bucket_message_div").html("Create New Bucket Error.");
      $("#new_bucket_set_name_button").button("enable");
      $("#new_bucket_set_acl_button").button("enable");
      $("#new_bucket_create_button").button("enable");
      $("#new_bucket_cancel_button").button("enable");
      addActionItem("CreateBucket", "Create Bucket "+$("#new_bucket_name")[0].value, "Fail");
    }
  });
}

function deleteBucket(){
  var reqString = buildRequest("DELETE",bucketList.buckets.bucket[current_bucket_index].name);
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    data: 'httpReq='+reqString,
    success: function(m){
      $("#delete_bucket_delete_button").button("enable");
      $("#delete_bucket_cancel_button").button("enable");
      if (m.httpResp.status=="600"){
        $("#delete_bucket_message_div").html("Delete Bucket Error : "+m.httpResp.body);
        addActionItem("Delete", "Delete Bucket "+bucketList.buckets.bucket[current_bucket_index].name, "Fail");
        return;
	  }
	  if (m.httpResp.status!="204"){
        var xmlData=xml2json.parser(m.httpResp.body);
        var error=xmlData.error;
        $("#delete_bucket_message_div").html("Delete Bucket Error : "+error.code+" : Message : "+error.messgae);
        addActionItem("Delete", "Delete Bucket "+bucketList.buckets.bucket[current_bucket_index].name, "Fail");
		return;
      }
      $("#new_bucket_message_div").html("Delete Bucket OK.");
      addActionItem("Delete", "Delete Bucket "+bucketList.buckets.bucket[current_bucket_index].name, "Success");

      getService();
      $("#delete_bucket_div").dialog( "close" );
    },
    error: function(){
      $("#new_bucket_message_div").html("Delete Bucket Error.");
      $("#delete_bucket_delete_button").button("enable");
      $("#delete_bucket_cancel_button").button("enable");
      addActionItem("Delete", "Delete Bucket "+bucketList.buckets.bucket[current_bucket_index].name, "Fail");
    }
  });
}

function getAcl(){
  var reqString = buildRequest("GET",normalizeObjectName(currentAclTarget)+"?acl");
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
//        $("#delete_bucket_message_div").html("Delete Bucket Error : "+m.httpResp.body);
		return;
	  }
      var xmlData=xml2json.parser(m.httpResp.body);
	  if (m.httpResp.status!="200"){
//        var error=xmlData.error;
//        $("#delete_bucket_message_div").html("Delete Bucket Error : "+error.code+" : Message : "+error.messgae);
		return;
      }
	  
      currentAclList=xmlData.accesscontrollist;
	  displayAclList();
    },
    error: function(){
//      $("#new_bucket_message_div").html("Delete Bucket Error.");
//      $("#delete_bucket_delete_button").button("enable");
//      $("#delete_bucket_cancel_button").button("enable");
    }
  });
}

function putAcl(){

  var aclListToPost = {owner: currentAclList.owner};
  aclListToPost.entrys = new Object();
  if (document.getElementById("acl_list")!=null) {
    aclListToPost.entrys.entry = new Array();
    viewAcls = document.getElementById("acl_list").getElementsByTagName("li");
    for (var i=0; i<viewAcls.length; i++){
      var obj=new Object();
      var inputItems = viewAcls[i].getElementsByTagName("input");
      for (var j=0; j<inputItems.length; j++){
        if (inputItems[j].type=="text"){
      	  obj.user=inputItems[j].value;
        } else if (inputItems[j].checked){
          obj.permission=inputItems[j].value;
        }
      }
      if (jQuery.trim(obj.user)!=""){
        aclListToPost.entrys.entry.push(obj);
      }
    }
  }

  options = {formatOutput: false,
             formatTextNodes: false,
             rootTagName:"AccessControlList",
			 indentString:"",
             replace:[{"owner":"Owner"},
                      {"entrys":"Entrys"},
                      {"entrys.entry":"Entry"},
                      {"entrys.entry.user":"User"},
                      {"entrys.entry.permission":"Permission"}
                     ],
             nodes:["owner",
                    "entrys",
                    "entrys.entry",
                    "entrys.entry.user",
                    "entrys.entry.permission"
                   ]
            };
  var xml = $.json2xml(aclListToPost, options);
  var reqString = buildRequest("PUT",normalizeObjectName(currentAclTarget)+"?acl",null,xml);
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
//        $("#delete_bucket_message_div").html("Delete Bucket Error : "+m.httpResp.body);
        addActionItem("Acl", "Set Acl /"+currentAclTarget, "Fail");
		return;
	  }
	  if (m.httpResp.status!="200"){
        var xmlData=xml2json.parser(m.httpResp.body);
//        var error=xmlData.error;
//        $("#delete_bucket_message_div").html("Delete Bucket Error : "+error.code+" : Message : "+error.messgae);
        addActionItem("Acl", "Set Acl /"+currentAclTarget, "Fail");
		return;
      }
      addActionItem("Acl", "Set Acl /"+currentAclTarget, "Success");
      getAcl();
    },
    error: function(){
      addActionItem("Acl", "Set Acl /"+currentAclTarget, "Fail");
      $("#new_bucket_message_div").html("Delete Bucket Error.");
      $("#delete_bucket_delete_button").button("enable");
      $("#delete_bucket_cancel_button").button("enable");
    }
  });
}

function getBucket(folder){
  $("#object_container").html("Loading Bucket Info ...");
  objectList=null;
  var reqString = buildRequest("GET",bucketList.buckets.bucket[current_bucket_index].name+"?delimiter=/&prefix="+normalizeObjectName(folder));
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
        $("#object_container").html("Loading Bucket Info Error : "+m.httpResp.body);
		return;
	  }
      var xmlData=xml2json.parser(m.httpResp.body);
	  if (m.httpResp.status!="200"){
        var error=xmlData.error;
        $("#object_container").html("Loading Bucket Info Error : <br>Code : "+error.code+"<br>Message : "+error.messgae);
		return;
      } 
      objectList = xmlData.listbucketresult;
	  if (typeof objectList.commonprefixes == "undefined") {
		  objectList.commonprefixes = new Array();
	  } else if(!(objectList.commonprefixes instanceof Array)) {
            var tmp = objectList.commonprefixes;
            objectList.commonprefixes = new Array();
            objectList.commonprefixes[0] = tmp;
	  }
	  if (typeof objectList.contents == "undefined") {
		  objectList.contents = new Array();
	  } else if(!(objectList.contents instanceof Array)) {
            var tmp = objectList.contents;
            objectList.contents = new Array();
            objectList.contents[0] = tmp;
	  }
	  loadObjectView();
      $("#bucket_id").html(bucketList.buckets.bucket[current_bucket_index].name+" >> /" + ((typeof objectList.prefix == "undefined")?"":objectList.prefix));
    },
    error: function(){
      $("#object_container").html("Loading Service Info Error.");
    }
  });
}

function deleteObject(path){
  var node = document.createElement("li");
  node.className = "oper_detail_item";
  node.innerHTML="Deleting "+path+" ...";
  document.getElementById("oper_delete_detail_list").appendChild(node);
  var reqString = buildRequest("DELETE",normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+path));
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    async: false,
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
        node.innerHTML="Delete Error : "+path;
        addActionItem("Delete", "Delete Object /"+bucketList.buckets.bucket[current_bucket_index].name+path, "Fail");
        return;
      }
      if (m.httpResp.status!="204"){
        node.innerHTML="Delete Error : "+path;
        addActionItem("Delete", "Delete Object /"+bucketList.buckets.bucket[current_bucket_index].name+path, "Fail");
        return;
      }
        node.innerHTML="Delete Finish : "+path;
        addActionItem("Delete", "Delete Object /"+bucketList.buckets.bucket[current_bucket_index].name+path, "Success");
    },
    error: function(){
      node.innerHTML="Delete Finish : "+path;
      addActionItem("Delete", "Delete Object /"+bucketList.buckets.bucket[current_bucket_index].name+path, "Fail");
    }
  });
}

function headObject(object){
  var reqString = buildRequest("HEAD",normalizeObjectName(object));
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
//        node.innerHTML="Delete Error : "+path;
        return;
      }
      if (m.httpResp.status!="200"){
//        node.innerHTML="Delete Error : "+path;
        return;
      }
	  displayObjectMeta(m.httpResp.head);
    },
    error: function(){
//      $("#object_container").html("Loading Service Info Error.");
    }
  });
}

function copyObject(dst, src){

  var node = document.createElement("li");
  node.className = "oper_detail_item";
  node.innerHTML="Copying "+src+" ...";
  document.getElementById("oper_copy_detail_list").appendChild(node);
  var head = {
      "rockstor-copy-source":normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+src),
      };
  var reqString = buildRequest("PUT",normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+dst),head);
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    async: false,
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
        node.innerHTML="Copy Error : "+src;
        addActionItem("Copy", "Copy Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
        return;
      }
      if (m.httpResp.status!="200"){
        node.innerHTML="Copy Error : "+src;
        addActionItem("Copy", "Copy Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
        return;
      }
      node.innerHTML="Copy Finish : "+src;
      addActionItem("Copy", "Copy Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Success");
	  
      var fullPath = (typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
      var vFolder = bucketVFolder[current_bucket_index].vFolder.get(fullPath);
      if (vFolder != null && vFolder.subFolders.length == 0) {
        bucketVFolder[current_bucket_index].vFolder.remove(fullPath);
      }
    },
    error: function(){
      node.innerHTML="Copy Error : "+src;
      addActionItem("Copy", "Copy Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
    }
  });
}

function moveObject(dst, src){
  var node = document.createElement("li");
  node.className = "oper_detail_item";
  node.innerHTML="Moving "+src+" ...";
  document.getElementById("oper_move_detail_list").appendChild(node);
  var head = {
      "rockstor-copy-source":normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+src),
      };
  var reqString = buildRequest("PUT",normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+dst),head);
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    async: false,
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
        node.innerHTML="Move Error : "+src;
        addActionItem("Move", "Move Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
        return;
      }
      if (m.httpResp.status!="200"){
        node.innerHTML="Move Error : "+src;
        addActionItem("Move", "Move Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
        return;
      }
      
      reqString = buildRequest("DELETE",normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+src));
      $.ajax({
        url: '/oper/RockStor.action',
        type: 'POST',
		async: false,
        data: 'httpReq='+reqString,
        success: function(m){
          if (m.httpResp.status=="600"){
            return;
          }
          if (m.httpResp.status!="204"){
            return;
          }
          node.innerHTML="Move Finish : "+src;
          addActionItem("Move", "Move Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Success");
		  
          var fullPath = (typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
          var vFolder = bucketVFolder[current_bucket_index].vFolder.get(fullPath);
          if (vFolder != null && vFolder.subFolders.length == 0) {
            bucketVFolder[current_bucket_index].vFolder.remove(fullPath);
          }
        },
        error: function(){
        }
      });
    },
    error: function(){
      node.innerHTML="Move Error : "+src;
      addActionItem("Move", "Move Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
    }
  });
}

function renameObject(dst, src){
  var head = {
      "rockstor-copy-source":normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+src),
      };
  var reqString = buildRequest("PUT",normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+dst),head);
  $.ajax({
    url: '/oper/RockStor.action',
    type: 'POST',
    async: false,
    data: 'httpReq='+reqString,
    success: function(m){
      if (m.httpResp.status=="600"){
        addActionItem("Rename", "Rename Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
        return;
      }
      if (m.httpResp.status!="200"){
        addActionItem("Rename", "Rename Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
        return;
      }
      
      reqString = buildRequest("DELETE",normalizeObjectName(bucketList.buckets.bucket[current_bucket_index].name+src));
      $.ajax({
        url: '/oper/RockStor.action',
        type: 'POST',
		async: false,
        data: 'httpReq='+reqString,
        success: function(m){
          if (m.httpResp.status=="600"){
            return;
          }
          if (m.httpResp.status!="204"){
            return;
          }
          addActionItem("Rename", "Rename Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Success");
        },
        error: function(){
        }
      });
    },
    error: function(){
      addActionItem("Rename", "Rename Object /"+bucketList.buckets.bucket[current_bucket_index].name+src, "Fail");
    }
  });
}
