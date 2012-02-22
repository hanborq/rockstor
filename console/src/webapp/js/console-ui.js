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

var key_ctrl=false;

var bottom_show="none";
var current_bucket="";
var current_bucket_index=-1;
var selected_objects=new Array();
var bucketVFolder = new Array();

var renaming_id = "";
var renaming_src_name = "";

var clipboard="";

var operTarget = {
  folder:"",
  list:new Array(),
};

var operObject = {
  folder:"",
  list:new Array(),
};


$(function() {

  $(document).bind("keydown", function(e){
   if (17 == e.which) {
       key_ctrl = true;
   }
  });

  $(document).bind("keyup", function(e){
   if (17 == e.which) {
       key_ctrl = false;
   }
  });

  $("#new_bucket_button").button();
  $("#new_folder_button").button();
  $("#upload_object_button").button();
  $("#refresh_button").button();
  $("#properties_button").button();
  $("#actions_button").button();
  $("#acl_add_button").button();
  $("#acl_save_button").button();
  $("#acl_cancel_button").button();
  $("#new_bucket_set_name_button").button();
  $("#new_bucket_set_acl_button").button();
  $("#new_bucket_create_button").button();
  $("#new_bucket_cancel_button").button();
  $("#delete_bucket_cancel_button").button();
  $("#delete_bucket_delete_button").button();
  $("#upload_file_cancel_button").button();
  $("#upload_file_upload_button").button();
  $("#upload_file_set_meta_button").button();
  $("#upload_file_set_acl_button").button();
  $("#upload_file_select_button").button();
  $("#upload_file_add_meta").button();
  $("#delete_object_cancel_button").button();
  $("#delete_object_delete_button").button();
  $("#copy_object_cancel_button").button();
  $("#copy_object_copy_button").button();
  $("#move_object_cancel_button").button();
  $("#move_object_move_button").button();
  
//  $("#SWFUpload_0").button();

  $(document).bind('click', function(e) {
    var $clicked = $(e.target);
    var clickID = $clicked[0].id;
    if (clickID!="create_folder_div" 
     && clickID!="create_folder_ul"
	 && clickID!="create_folder_li"
	 && clickID!="create_folder_png"
	 && clickID!="create_folder_name"
	 && clickID!="create_folder_ok"
	 && clickID!="create_folder_cancel") {
	  $("#create_folder_div").hide("fade",500);
	}
	
    if (renaming_id!="") {
      if (clickID!="rename_ok" 
		  && clickID!="rename_cancel"
		  && clickID!="rename_input") {
	  	  renameCancel();
	  }
    }
	
  });
/******************* create folder div *******************/

  $("#create_folder_name").bind("keypress", function(e){
    if (13 == e.which) {
       createFolder();
    }
  });

/******************* rename folder div *******************/
  $("#rename_object_div" ).dialog({
    autoOpen: false,
    height: 100,
    width: 300,
    modal: true,
    title: "Rename Object",
    hide: 'slide',
    show: 'slide',
  });
/******************* create bucket div *******************/
  $("#create_bucket_div" ).dialog({
    autoOpen: false,
    height: 350,
    width: 600,
    modal: true,
    title: "Create Bucket",
    hide: 'slide',
    show: 'slide',
    open: function(event, ui){
            $("#new_bucket_name")[0].value="";
            document.getElementsByName("new_bucket_set_acl")[0].checked=true;
            $("#new_bucket_set_name_button").button("enable");
            $("#new_bucket_set_acl_button").button("enable");
            $("#new_bucket_create_button").button("enable");
            $("#new_bucket_cancel_button").button("enable");
            $("#new_bucket_set_name_button").hide();
            $("#new_bucket_set_acl_button").show();
            $("#new_bucket_set_name_div").show();
            $("#new_bucket_set_acl_div").hide();
            $("#new_bucket_message_div").html("");
    },
  });

  $("#new_bucket_set_name_button").bind("click", function(e){
            $("#new_bucket_set_name_button").hide();
            $("#new_bucket_set_acl_button").show();
            $("#new_bucket_set_acl_div").hide();
            $("#new_bucket_set_name_div").show("fade", 500);
  });

  $("#new_bucket_set_acl_button").bind("click", function(e){
            $("#new_bucket_set_name_button").show();
            $("#new_bucket_set_acl_button").hide();
            $("#new_bucket_set_name_div").hide();
            $("#new_bucket_set_acl_div").show("fade", 500);
  });

  $("#new_bucket_create_button").bind("click", function(e){
            $("#new_bucket_name")[0].value=jQuery.trim($("#new_bucket_name")[0].value);
            if ($("#new_bucket_name")[0].value=="") {
              $("#new_bucket_message_div").html("bucket name cannot be empty");
			  return;
            }
            $("#new_bucket_set_name_button").button("disable");
            $("#new_bucket_set_acl_button").button("disable");
            $("#new_bucket_create_button").button("disable");
            $("#new_bucket_cancel_button").button("disable");
            putBucket();
  });

  $("#new_bucket_cancel_button").bind("click", function(e){
            $("#create_bucket_div").dialog( "close" );
  });

/******************* delete bucket div *******************/
  $("#delete_bucket_div" ).dialog({
    autoOpen: false,
    height: 350,
    width: 600,
    modal: true,
    title: "Delete Bucket",
    hide: 'slide',
    show: 'slide',
    open: function(event, ui){
            $("#delete_bucket_name").html(bucketList.buckets.bucket[current_bucket_index].name);
            $("#delete_bucket_delete_button").button("enable");
            $("#delete_bucket_cancel_button").button("enable");
            $("#delete_bucket_message_div").html("");
    },
  });
  
  $("#delete_bucket_delete_button").bind("click", function(e){
            $("#delete_bucket_delete_button").button("disable");
            $("#delete_bucket_cancel_button").button("disable");
            deleteBucket();
  });
  
  $("#delete_bucket_cancel_button").bind("click", function(e){
            $("#delete_bucket_div").dialog( "close" );
  });

/******************* upload file div *******************/
  $( "#upload_file_div" ).dialog({
    autoOpen: false,
    height: 350,
    width: 600,
    modal: true,
    title: "Upload Files",
    hide: 'slide',
    show: 'slide',
    open: function(event, ui){
            $("#upload_file_select_div").html("");
            $("#upload_file_set_meta_container").html("No Meta Items");
            $("#upload_file_top_div").html("<span id='select_file_button'> </span>");
            document.getElementsByName("upload_file_set_acl")[0].checked=true;
			uploadingArray = new Array();
			initSU();
            $("#upload_file_cancel_button").button("enable");
            $("#upload_file_upload_button").button("enable");
            $("#upload_file_set_meta_button").button("enable");
            $("#upload_file_set_acl_button").button("enable");
            $("#upload_file_select_button").button("enable");
            $("#upload_file_select_button").hide();
            $("#upload_file_set_acl_button").show();
            $("#upload_file_set_meta_button").hide();
            $("#upload_file_select_div").show();
            $("#upload_file_set_acl_div").hide();
            $("#upload_file_set_meta_div").hide();
            $("#upload_file_process_div").hide();
    },
    close: function(event, ui){
            refreshObjectView();
    }
  });

  $("#upload_file_select_button").bind("click", function(e){
            $("#upload_file_select_button").hide();
            $("#upload_file_set_acl_button").show();
            $("#upload_file_set_meta_button").hide();
            $("#upload_file_select_div").show("fade", 500);
            $("#upload_file_set_acl_div").hide();
            $("#upload_file_set_meta_div").hide();
  });

  $("#upload_file_set_acl_button").bind("click", function(e){
            $("#upload_file_select_button").show();
            $("#upload_file_set_acl_button").hide();
            $("#upload_file_set_meta_button").show();
            $("#upload_file_select_div").hide();
            $("#upload_file_set_acl_div").show("fade", 500);
            $("#upload_file_set_meta_div").hide();
  });

  $("#upload_file_set_meta_button").bind("click", function(e){
            $("#upload_file_select_button").hide();
            $("#upload_file_set_acl_button").show();
            $("#upload_file_set_meta_button").hide();
            $("#upload_file_select_div").hide();
            $("#upload_file_set_acl_div").hide();
            $("#upload_file_set_meta_div").show("fade", 500);
  });

  $("#upload_file_upload_button").bind("click", function(e){
            $("#upload_file_select_div").hide();
            $("#upload_file_set_acl_div").hide();
            $("#upload_file_set_meta_div").hide();
            $("#upload_file_process_div").show("fade", 500);
            $("#upload_file_upload_button").button("disable");
            $("#upload_file_set_meta_button").button("disable");
            $("#upload_file_set_acl_button").button("disable");
            $("#upload_file_select_button").button("disable");
            initProcessView();
			setUploadMetaData();
            swfu.startUpload();
  });

  $("#upload_file_cancel_button").bind("click", function(e){
            uploadCancelAll();
            $("#upload_file_div").dialog( "close" );
  });

  $("#upload_file_add_meta").bind("click", function(e){
            var ts=new Date().getTime();
            if (document.getElementById("upload_file_meta_list")==null) {
              document.getElementById("upload_file_set_meta_container").innerHTML="<ul id='upload_file_meta_list' class='upload_file_meta_list'><li class='upload_file_meta_item' id='upload_file_meta_item_"+ts+"'>Key : rockstor-meta-<input id='upload_file_meta_item_key_"+ts+"' type='text' style='border:#CCCCCC thin solid; width:100px; height:14px; font-size:12px; color:#666666'/> Value : <input id='upload_file_meta_item_value_"+ts+"' type='text' style='border:#CCCCCC thin solid; width:100px; height:14px; font-size:12px; color:#666666'/><img style='float:right' src='images/cross.png' onclick=javascript:delMetaItem('"+ts+"')></li></ul>";
            } else {
              var ts=new Date().getTime();
              var metaNode=document.createElement("li");
              metaNode.className='upload_file_meta_item';
              metaNode.id='upload_file_meta_item_'+ts;
              metaNode.innerHTML="Key : rockstor-meta-<input id='upload_file_meta_item_key_"+ts+"' type='text' style='border:#CCCCCC thin solid; width:100px; height:14px; font-size:12px; color:#666666'/> Value : <input id='upload_file_meta_item_value_"+ts+"' type='text' style='border:#CCCCCC thin solid; width:100px; height:14px; font-size:12px; color:#666666'/><img style='float:right' src='images/cross.png' onclick=javascript:delMetaItem('"+ts+"')>";
             document.getElementById("upload_file_meta_list").appendChild(metaNode);
            }
  });

/******************* delete object div *******************/
  $( "#delete_object_div" ).dialog({
    autoOpen: false,
    height: 350,
    width: 600,
    modal: true,
    title: "Delete Objects",
    hide: 'slide',
    show: 'slide',
    open: function(event, ui){
            $("#delete_bucket_tip_div").show();
            $("#delete_object_progress_div").hide();
            $("#delete_object_detail_div").hide();
            $("#delete_object_cancel_button").button("enable");
            $("#delete_object_delete_button").button("enable");
    },
    close: function(event, ui){
            operTarget = {
              folder:"",
              list:new Array(),
            };
            operObject = {
              folder:"",
              list:new Array(),
            };
    }
  });

  $("#delete_object_delete_button").bind("click", function(e){
            $("#delete_bucket_tip_div").hide();
            $("#delete_object_progress_div").show();
            $("#delete_object_detail_div").show();
            $("#delete_object_delete_button").button("disable");
            $("#delete_object_progress").progressbar({
              value:0
            });
            buildOperObject();
            var detailView = "<ul id='oper_delete_detail_list' class='oper_detail_list'>";
            detailView += "<ul>";
            $("#delete_object_detail_div").html(detailView);
            for (var i=0; i<operObject.list.length; i++) {
              if (operObject.list[i].real) {
                deleteObject(operObject.list[i].path);
              } else {
				  bucketVFolder[current_bucket_index].vFolder.remove(operObject.list[i].path);
              }
              var percent = Math.ceil(((i+1) / operObject.list.length) * 100);
              $("#delete_object_progress").progressbar({
                value:percent
              });
            }
            $("#delete_object_cancel_button").button("enable");
  });
  
  $("#delete_object_cancel_button").bind("click", function(e){
            refreshObjectView();
            $("#delete_object_div").dialog( "close" );
  });

/******************* copy object div *******************/
  $( "#copy_object_div" ).dialog({
    autoOpen: false,
    height: 350,
    width: 600,
    modal: true,
    title: "Copy Objects",
    hide: 'slide',
    show: 'slide',
    open: function(event, ui){
            $("#copy_object_tip_div").show();
            $("#copy_object_progress_div").hide();
            $("#copy_object_detail_div").hide();
            $("#copy_object_cancel_button").button("enable");
            $("#copy_object_copy_button").button("enable");
			checkCopyRight();
    },
    close: function(event, ui){
            operTarget = {
              folder:"",
              list:new Array(),
            };
            operObject = {
              folder:"",
              list:new Array(),
            };
			clipboard="";
            refreshObjectView();
    }
  });

  $("#copy_object_copy_button").bind("click", function(e){
            $("#copy_object_tip_div").hide();
            $("#copy_object_progress_div").show();
            $("#copy_object_detail_div").show();
            $("#copy_object_copy_button").button("disable");
            $("#copy_object_cancel_button").button("disable");
            $("#copy_object_progress").progressbar({
              value:0
            });
            buildOperObject();
            var detailView = "<ul id='oper_copy_detail_list' class='oper_detail_list'>";
            detailView += "<ul>";
            $("#copy_object_detail_div").html(detailView);
            for (var i=0; i<operObject.list.length; i++) {
              if (operObject.list[i].real) {
                var srcDispName = operObject.folder==""?srcDispName = operObject.list[i].path:operObject.list[i].path.substr(operObject.folder.length+1);
                var dst = ((typeof objectList.prefix == "undefined")?"":"/"+objectList.prefix.substr(0, objectList.prefix.length-1)) + srcDispName;
                copyObject(dst, operObject.list[i].path);
              } else {
                 var dst = ((typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1));
                 var name = operObject.list[i].path.substr(operObject.list[i].path.lastIndexOf("/")+1);
				 var path = (dst=="")?name:dst+"/"+name;
				 bucketVFolder[current_bucket_index].vFolder.add(path);
              }
              var percent = Math.ceil(((i+1) / operObject.list.length) * 100);
              $("#copy_object_progress").progressbar({
                value:percent
              });
            }
            $("#copy_object_cancel_button").button("enable");
  });
  
  $("#copy_object_cancel_button").bind("click", function(e){
            $("#copy_object_div").dialog( "close" );
  });


/******************* move object div *******************/
  $( "#move_object_div" ).dialog({
    autoOpen: false,
    height: 350,
    width: 600,
    modal: true,
    title: "Move Objects",
    hide: 'slide',
    show: 'slide',
    open: function(event, ui){
            $("#move_object_tip_div").show();
            $("#move_object_progress_div").hide();
            $("#move_object_detail_div").hide();
            $("#move_object_cancel_button").button("enable");
            $("#move_object_move_button").button("enable");
			checkMoveRight();
    },
    close: function(event, ui){
            operTarget = {
              folder:"",
              list:new Array(),
            };
            operObject = {
              folder:"",
              list:new Array(),
            };
			clipboard="";
            refreshObjectView();
    }
  });

  $("#move_object_move_button").bind("click", function(e){
            $("#move_object_tip_div").hide();
            $("#move_object_progress_div").show();
            $("#move_object_detail_div").show();
            $("#move_object_move_button").button("disable");
            $("#move_object_cancel_button").button("disable");
            $("#move_object_progress").progressbar({
              value:0
            });
            buildOperObject();
            var detailView = "<ul id='oper_move_detail_list' class='oper_detail_list'>";
            detailView += "<ul>";
            $("#move_object_detail_div").html(detailView);
            for (var i=0; i<operObject.list.length; i++) {
              if (operObject.list[i].real) {
                var srcDispName = operObject.folder==""?srcDispName = operObject.list[i].path:operObject.list[i].path.substr(operObject.folder.length+1);
                var dst = ((typeof objectList.prefix == "undefined")?"":"/"+objectList.prefix.substr(0, objectList.prefix.length-1)) + srcDispName;
                moveObject(dst, operObject.list[i].path);
              } else {
                 var dst = ((typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1))
                 var name = operObject.list[i].path.substr(operObject.list[i].path.lastIndexOf("/")+1);
                 var path = (dst=="")?name:dst+"/"+name;
                 bucketVFolder[current_bucket_index].vFolder.add(path);
                 bucketVFolder[current_bucket_index].vFolder.remove(operObject.list[i].path);
              }
              var percent = Math.ceil(((i+1) / operObject.list.length) * 100);
              $("#move_object_progress").progressbar({
                value:percent
              });
            }
            $("#move_object_cancel_button").button("enable");
  });
  
  $("#move_object_cancel_button").bind("click", function(e){
            $("#move_object_div").dialog( "close" );
  });

/*********************************************************/


  $("#properties_operation_div").tabs();

  $("#acl_add_button").bind("click", function(e){
    addAclItem();
  });
  $("#acl_cancel_button").bind("click", function(e){
    displayAclList();
  });
  $("#acl_save_button").bind("click", function(e){
    putAcl();
  });

});

function loadBucketSelectEffect() {
  $(".bucket_item").bind('mousedown', function(e) {
      if (3 == e.which) {
        $(e.target).click();
      }
  });
  $("#bucket_list").bind('click', function(e) {
        var $clicked = $(e.target);
        var clickID = $clicked[0].id;
        if (current_bucket!=""){
          $(document.getElementById("bucket_"+current_bucket_index)).removeClass("bucket_selected");
          $(document.getElementById("bucket_"+current_bucket_index)).addClass("bucket_unselected");
        } else {
        }
        $clicked.removeClass("bucket_unselected");
        $clicked.addClass("bucket_selected");
        current_bucket_index = new Number(clickID.substr(7));
        current_bucket = bucketList.buckets.bucket[current_bucket_index].name;
        getBucket("");
        if (bottom_show=="properties") {
          showPropertiesDiv(true);
        }
  });
}

function loadBucketContextMenu() {
  $('li.bucketMenu').contextMenu('menu_bucket', {
      bindings: {
        'menu_bucket_delete': function(t) {
          showDeleteBucket();
        },
        'menu_bucket_properties': function(t) {
          showPropertiesDiv(true);
        },
      },
  });
}

function loadBucketContextMenu2() {
  $("#bucket_container").contextMenu('menu_bucket2',  {
      bindings: {
        'menu_bucket2_create': function(t) {
          showNewBucket();
        },
        'menu_bucket2_refresh': function(t) {
          getService();
        },
      },
  });
} 

function loadObjectSelectEffect() {
	
//  $("li.object_item_size").bind("click", function(e) {
//    $(e.target).parent().click();
//  });
//  $("li.object_item_time").bind("click", function(e) {
//    $(e.target).parent().click();
//  });
	
  $(".object_item").bind('mousedown', function(e) {
      if(3 == e.which) {
        var id = null;
        if ($(e.target).hasClass("object_item_name") || $(e.target).hasClass("object_item_time") || $(e.target).hasClass("object_item_size")) {
          id = $(e.target).parent()[0].id;
        } else {
          id = $(e.target)[0].id
        }
        for (var i=0; i<selected_objects.length; i++){
          if (selected_objects[i] == id) {
            return;
          }
        }
        $(document.getElementById(id)).click();
      }
  });
  
  $("#object_list").bind('click', function(e) {
    var $clicked = $(e.target);
    if ($(e.target).hasClass("object_item_name") || $(e.target).hasClass("object_item_time") || $(e.target).hasClass("object_item_size")) {
      $clicked.parent().click();
	  return;
    }
    var clickID = $clicked[0].id;
    if (!key_ctrl) {
      selected_objects = new Array();
      $(".object_item").removeClass("object_selected");
      $(".object_item").addClass("object_unselected");
    }
	
    if ($clicked.hasClass("object_selected")) {
      $clicked.removeClass("object_selected");
      $clicked.addClass("object_unselected");
    } else {
      $clicked.removeClass("object_unselected");
      $clicked.addClass("object_selected");
    }
    selected_objects.push(clickID);
    if (bottom_show=="properties") {
      showPropertiesDiv();
    }
    loadObjectContextMenu();
  });
  
  $("#object_list").bind('dblclick', function(e) {
    var $clicked = $(e.target);
    var clickID = $clicked[0].id;

    if (clickID=="parent_folder"){
      var lastIndex = objectList.prefix.lastIndexOf("/", (objectList.prefix.length - 2));
      if (lastIndex==-1) {
        getBucket("");
      } else {
        getBucket(objectList.prefix.substring(0, lastIndex+1));
      }
    } else if (clickID.indexOf("real_folder_")==0) {
      var index = Number(clickID.substr(12));
      getBucket(objectList.commonprefixes[index].prefix.substr(1));
    } else if (clickID.indexOf("v_folder_")==0) {
      var index = Number(clickID.substr(9));
      if (typeof objectList.prefix == "undefined") {
        var vfs = bucketVFolder[current_bucket_index].vFolder.getFolders("");
        getBucket(vfs[index].name+"/");
	  } else {
        var currentFolder = objectList.prefix.substr(0, objectList.prefix.length-1);
        var vfs = bucketVFolder[current_bucket_index].vFolder.getFolders(currentFolder);
        getBucket(currentFolder+"/"+vfs[index].name+"/");
      }
    }
  });
}

function loadObjectContextMenu() {

  var menuView = "";
  if (selected_objects.length==1) {
    if (selected_objects[0].indexOf("real_folder_")==0 || selected_objects[0].indexOf("v_folder_")==0 ) {
      menuView += "<li id='menu_object_open'><img src='images/open.png' />Open</li>";
    }
    menuView += "<li id='menu_object_delete'><img src='images/delete.png' />Delete</li>"
    menuView += "<li id='menu_object_rename'><img src='images/rename.png' />Rename</li>"
    menuView += "<li id='menu_object_cut'><img src='images/cut.png' />Cut</li>"
    menuView += "<li id='menu_object_copy'><img src='images/copy.png' />Copy</li>"
    menuView += "<li id='menu_object_properties'><img src='images/properties.png' />Properties</li>"
  } else if (selected_objects.length > 1) {
    menuView += "<li id='menu_object_delete'><img src='images/delete.png' />Delete</li>"
    menuView += "<li id='menu_object_cut'><img src='images/cut.png' />Cut</li>"
    menuView += "<li id='menu_object_copy'><img src='images/copy.png' />Copy</li>"
    menuView += "<li id='menu_object_properties'><img src='images/properties.png' />Properties</li>"
  }
  
  $("#menu_object_list").html(menuView);
 
  $('li.objectMenu').contextMenu('menu_object', {
    bindings: {
        'menu_object_open': function(t) {
			$(document.getElementById(t.id)).dblclick();
        },
        'menu_object_delete': function(t) {
          showDeleteObjectsDiv();
        },
        'menu_object_rename': function(t) {
          showRename();
        },
        'menu_object_cut': function(t) {
          buildOperTarget();
          clipboard="move";
        },
        'menu_object_copy': function(t) {
          buildOperTarget();
          clipboard="copy";
        },
        'menu_object_properties': function(t) {
          showPropertiesDiv();
        },
    },
  });
}

function loadObjectContextMenu2() {
  var menuView = "";
  menuView += "<li id='menu_object2_create'><img src='images/create-folder.png' />New Folder</li>"
  menuView += "<li id='menu_object2_upload'><img src='images/upload.png' />Upload</li>"
  if (clipboard!="" && operTarget.folder!=((typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1))) {
    menuView += "<li id='menu_object2_paste'><img src='images/paste.png' />Paste</li>"
  }
  $("#menu_object2_list").html(menuView);
  $('#object_container').contextMenu('menu_object2', {
    bindings: {
        'menu_object2_create': function(t) {
          showCreateFolderDiv();
        },
        'menu_object2_upload': function(t) {
          showUploadFileDiv();
        },
        'menu_object2_paste': function(t) {
          if (clipboard=="copy") {
            showCopyObjectDiv();
          } else if (clipboard=="move") {
            showMoveObjectDiv();
          }
        },
    },
  });
}

function checkCopyRight() {
  for (var i=0; i<operTarget.list.length; i++) {
    var path = operTarget.list[i].path;
    var currentPath = (typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
    if (currentPath != "" && path.indexOf(currentPath)!=-1) {
      $("#copy_object_tip_div").html("The selected items cannot be copied to here.");
      $("#copy_object_copy_button").button("disable");
      return;
    }
  }
  $("#copy_object_copy_button").button("enable");
  $("#copy_object_tip_div").html("Are you sure to copy seleted items to here?");
}

function checkMoveRight() {
  for (var i=0; i<operTarget.list.length; i++) {
    var path = operTarget.list[i].path;
    var currentPath = (typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
    if (currentPath != "" && path.indexOf(currentPath)!=-1) {
      $("#move_object_tip_div").html("The selected items cannot be moved to here.");
      $("#move_object_move_button").button("disable");
      return;
    }
  }
  $("#move_object_move_button").button("enable");
  $("#move_object_tip_div").html("Are you sure to move seleted items to here?");
}

function refreshObjectView() {
  getBucket((typeof objectList.prefix == "undefined")?"":objectList.prefix);
}

function showUploadFileDiv(){
  $("#upload_file_div").dialog( "open" );
}

function showNewBucket(){
  $("#create_bucket_div" ).dialog( "open" );
}

function showDeleteBucket(){
  $("#delete_bucket_div" ).dialog( "open" );
}

function showCopyObjectDiv(){
  $("#copy_object_div").dialog( "open" );
}

function showMoveObjectDiv(){
  $("#move_object_div").dialog( "open" );
}

function showCreateFolderDiv(){
  $("#create_folder_name")[0].value="";
  $("#create_folder_div").show("fade",500);
  $("#create_folder_name").focus()
}

function showRename() {
  var view = "<input id='rename_input' type='text' style='border:#CCCCCC thin solid; width:300px; height:14px; font-size:12px; color:#666666'/> ";	
  view += "<img id='rename_ok' src='images/tick.png' align='absmiddle' onclick='javascript:renameWork()' />"; 
  view += "<img id='rename_cancel' src='images/cross.png' align='absmiddle' onclick='javascript:renameCancel()' />";
  var id = selected_objects[0];
  renaming_id=id;
  var index = -1;
  if (id.indexOf("v_folder_")==0) {
    view = "<img src='images/folder.png' align='absmiddle' /> "+view;  
    $(document.getElementById(id)).html(view);
	index = Number(id.substr(9));
    var fullPath = (typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
    var vFolders = bucketVFolder[current_bucket_index].vFolder.getFolders(fullPath);
    renaming_src_name = vFolders[index].name;
  } else if (id.indexOf("real_folder_")==0) {
    view = "<img src='images/folder.png' align='absmiddle' /> "+view;  
    $(document.getElementById(id)).html(view);
    index = Number(id.substr(12));
    var prefixes = objectList.commonprefixes[index].prefix.split("/");
    renaming_src_name = prefixes[prefixes.length-2];
  } else if (id.indexOf("object_")==0) {
    view = "<img src='images/object.png' align='absmiddle' /> "+view;  
    $(document.getElementById(id+"_name")).html(view);
    index = Number(id.substr(7));
    var prefixes = objectList.contents[index].key.split("/");
    renaming_src_name = prefixes[prefixes.length-1];
  }
  document.getElementById("rename_input").value=renaming_src_name;
  $("#rename_input").bind("keypress", function(e){
    if (13 == e.which) {
       renameWork();
    }
  });
}

function renameCancel() {
  var view = renaming_src_name;
  if (renaming_id.indexOf("v_folder_")==0) {
    view = "<img src='images/folder.png' align='absmiddle' /> "+view;  
    $(document.getElementById(renaming_id)).html(view);
  } else if (renaming_id.indexOf("real_folder_")==0) {
    view = "<img src='images/folder.png' align='absmiddle' /> "+view;  
    $(document.getElementById(renaming_id)).html(view);
  } else if (renaming_id.indexOf("object_")==0) {
    view = "<img src='images/object.png' align='absmiddle' /> "+view;  
    $(document.getElementById(renaming_id+"_name")).html(view);
  }
  renaming_src_name = "";
  renaming_id = "";
}

function renameWork() {
  
  var newName = jQuery.trim(document.getElementById("rename_input").value);
  if (newName=="" || newName==renaming_src_name) {
    renameCancel();
	return;
  }
  
  $("#rename_object_div" ).dialog("open");
  if (renaming_id.indexOf("v_folder_")==0) {
    var index = Number(renaming_id.substr(9));
    var fullPath = (typeof objectList.prefix == "undefined")?"":objectList.prefix;
	var vFolder = bucketVFolder[current_bucket_index].vFolder;
    if (vFolder.exist(fullPath+newName)) {
    } else {
      vFolder.add(fullPath+newName)
    }
    vFolder.remove(fullPath+renaming_src_name);
  } else if (renaming_id.indexOf("real_folder_")==0 || renaming_id.indexOf("object_")==0) {
    selected_objects.push(renaming_id);
    buildOperTarget();
    buildOperObject();
	var fullPath = (typeof objectList.prefix == "undefined")?"":objectList.prefix;
    for (var i=0; i<operObject.list.length; i++) {
      if (operObject.list[i].real) {
        var oldNamePath = fullPath + renaming_src_name;
        var newNamePath = fullPath + newName;
        newNamePath = operObject.list[i].path.replace(oldNamePath, newNamePath);
        renameObject(newNamePath, operObject.list[i].path);
      }
    }
  }
  renaming_src_name = "";
  renaming_id = "";
  selected_objects=new Array();
  operTarget = {
    folder:"",
    list:new Array(),
  };
  operObject = {
    folder:"",
    list:new Array(),
  }; 
  refreshObjectView();
  $("#rename_object_div" ).dialog("close");
}

function showBottomDiv(){
	$("#main_top").removeClass("main_top");
	$("#main_top").addClass("main_top_show_bottom");
    $("#main_top").css("height", pageHeight-40-200);
	$("#main_bottom").show("explode", 500);
}

function hideBottomDiv(){
	$("#main_bottom").hide("explode", 500);
	$("#main_top").removeClass("main_top_show_bottom");
	$("#main_top").addClass("main_top");
    $("#main_top").css("height", pageHeight-40);
	bottom_show="none";
}

function showActionsDiv(){
    if (bottom_show=="actions") {
        return;
    }
	if (bottom_show=="properties") {
		$("#properties_div").hide("fade", 500, function(){
            $("#actions_div").show("fade", 500);
            bottom_show="actions";
		});
	} else {
        $("#actions_div")[0].style.display="block";
  	    $("#properties_div")[0].style.display="none";
        bottom_show="actions"
        showBottomDiv();
	}
}

function showPropertiesDiv(showBucket){
  if (bottom_show=="properties") {
     //
  } else { 
    if (bottom_show=="actions") {
      $("#actions_div").hide("fade", 500, function(){
      $("#properties_div").show("fade", 500);
        bottom_show="properties";
      });
    } else {
      $("#properties_div")[0].style.display="block";
      $("#actions_div")[0].style.display="none";
      bottom_show="properties"
      showBottomDiv();
    }
  }
  if (current_bucket_index>-1) {
    if (showBucket){
      showBucketProperty();
	} else {
      showObjectProperty();
    }
  }
}

function showBucketProperty(){
  var basicInfoView="<table class='table_properties'>";
  basicInfoView+="<tr><td width='100'>bucket</td><td width='300'>"+bucketList.buckets.bucket[current_bucket_index].name+"</td></tr>";
  basicInfoView+="<tr><td>owner</td><td>"+bucketList.owner+"</td></tr>";
  basicInfoView+="<tr><td>create</td><td>"+bucketList.buckets.bucket[current_bucket_index].creationdate+"</td></tr>";
  basicInfoView+="</table>";
  $("#bottom_properties_info").html(basicInfoView);
  $("#properties_operation_div").show();
  currentAclTarget=bucketList.buckets.bucket[current_bucket_index].name;
  $("#tabs-detail").html("No detail info.");
  getAcl();
}

function showObjectProperty(){
  if (selected_objects.length < 1) {
    showBucketProperty();
  } else if (selected_objects.length > 1) {
    $("#properties_operation_div").hide();
    var objectInfoView="<table class='table_properties'>";
    objectInfoView += "<tr><td>seleted "+selected_objects.length+" items</td></tr>";
    objectInfoView += "</table>";
  } else if (selected_objects[0].indexOf("v_folder_") == 0){
    $("#properties_operation_div").hide();
    var index = Number(selected_objects[0].substr(9));
    var p = (typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
    var path = ((typeof objectList.prefix == "undefined")?"":objectList.prefix)+bucketVFolder[current_bucket_index].vFolder.get(p).subFolders[index].name;
    var objectInfoView="<table class='table_properties'>";
    objectInfoView+="<tr><td width='100'>bucket</td><td width='300'>"+bucketList.buckets.bucket[current_bucket_index].name+"</td></tr>";
    objectInfoView+="<tr><td>folder</td><td>"+path+"<br>(tmp)</td></tr>";
    objectInfoView += "</table>";
  } else if (selected_objects[0].indexOf("real_folder_") == 0) {
    $("#properties_operation_div").hide();
    var index = Number(selected_objects[0].substr(12));
    var objectInfoView="<table class='table_properties'>";
    objectInfoView+="<tr><td width='100'>bucket</td><td width='300'>"+bucketList.buckets.bucket[current_bucket_index].name+"</td></tr>";
    objectInfoView+="<tr><td>folder</td><td>"+objectList.commonprefixes[index].prefix+"</td></tr>";
    objectInfoView += "</table>";
  } else if (selected_objects[0].indexOf("object_") == 0){
    $("#properties_operation_div").show();
    var index = Number(selected_objects[0].substr(7));
    var path = objectList.contents[index].key;
    var objectInfoView="<table class='table_properties'>";
    objectInfoView+="<tr><td width='100'>bucket</td><td width='300'>"+bucketList.buckets.bucket[current_bucket_index].name+"</td></tr>";
    objectInfoView+="<tr><td>object</td><td>"+objectList.contents[index].key+"</td></tr>";
    objectInfoView+="<tr><td>owner</td><td>"+objectList.contents[index].owner+"</td></tr>";
    objectInfoView+="<tr><td>size</td><td>"+objectList.contents[index].size+"</td></tr>";
    objectInfoView+="<tr><td>etag</td><td>"+objectList.contents[index].etag+"</td></tr>";
    objectInfoView+="<tr><td>modified</td><td>"+objectList.contents[index].lastmodified+"</td></tr>";
    objectInfoView += "</table>";
    currentAclTarget=bucketList.buckets.bucket[current_bucket_index].name+objectList.contents[index].key;
    headObject(currentAclTarget);
    getAcl(); 
  }
  $("#bottom_properties_info").html(objectInfoView);
}

function displayAclList(){
  var aclView="";
  if (typeof currentAclList.entrys == "undefined") {
    aclView="No acl defined.";
  } else if (typeof currentAclList.entrys.entry == "undefined") {
    aclView="No acl defined.";
  } else {
    if (!(currentAclList.entrys.entry instanceof Array)){
      var tmp = currentAclList.entrys.entry;
      currentAclList.entrys.entry = new Array();
      currentAclList.entrys.entry[0]=tmp;
    }
    var aclView="<ul id='acl_list' class='acl_list'>";
    for (var i=0; i<currentAclList.entrys.entry.length; i++){
      aclView+="<li id='acl_item_"+i+"' class='acl_item'><input type='text' value='"+currentAclList.entrys.entry[i].user+"'> "+genAclItemRadioView(i,currentAclList.entrys.entry[i].permission)+"<img style='float:right' src='images/cross.png' onclick=javascript:delAclItem('acl_item_"+i+"')></li>";
    }
    aclView+="</ul>";
  }
  $("#bottom_properties_acl_list").html(aclView);
}

function displayObjectMeta(heads) {
  var date = buildTimeString();
  var authorization = buildAuthorizationString("GET",date);
  var downloadUrl = publicSite+currentAclTarget +"?Date="+encodeURIComponent(encode64(date))+"&Authorization="+encodeURIComponent(encode64(authorization));
  var metaView = "<table class='table_meta'>";
  metaView += "<tr><td width='200'>url</td><td width='500'><a target='_blank' href='"+downloadUrl+"'>"+publicSite+currentAclTarget+"</a></td></tr>";
  if (typeof heads["Content-Type"] != "undefined") {
    metaView += "<tr><td>Content-Type</td><td>"+heads["Content-Type"]+"</td></tr>";
  }
  for (var head in heads) {
    if (head.indexOf("rockstor-meta-")==0) {
       metaView += "<tr><td>"+head+"</td><td>"+heads[head]+"</td></tr>";
    }
  }
  metaView += "</table>";
  $("#tabs-detail").html(metaView);
}

function addAclItem(){
  if (document.getElementById("acl_list")==null) {
    var aclList = "<ul id='acl_list' class='acl_list'></ul>";
    document.getElementById("bottom_properties_acl_list").innerHTML=aclList;
  }
  var id=new Date().getTime();
  var aclNode=document.createElement("li");
  aclNode.className='acl_item';
  aclNode.id='acl_item_'+id;
  aclNode.innerHTML="<input type='text' value=''> "+genAclItemRadioView(id) + "<img style='float:right' src='images/cross.png' onclick=javascript:delAclItem('"+aclNode.id+"')>";
  document.getElementById("acl_list").appendChild(aclNode);
}

function delAclItem(id){
  document.getElementById("acl_list").removeChild(document.getElementById(id));
  if (document.getElementById("acl_list").getElementsByTagName("li").length==0) {
    document.getElementById("bottom_properties_acl_list").removeChild(document.getElementById("acl_list"));
  }
}

function genAclItemRadioView(id,acl){
  if (acl==null)
    acl="READ";
  if (acl=="READ") {
    return "<input type='radio' name='acl_"+id+"' value='READ' checked='checked'> READ <input type='radio' name='acl_"+id+"' value='WRITE' > WRITE <input type='radio' name='acl_"+id+"' value='FULL_CONTROL' > FULL_CONTROL";
  }else if (acl=="WRITE") {
    return "<input type='radio' name='acl_"+id+"' value='READ'> READ <input type='radio' name='acl_"+id+"' value='WRITE' checked='checked'> WRITE <input type='radio' name='acl_"+id+"' value='FULL_CONTROL' > FULL_CONTROL";
  }else if (acl=="FULL_CONTROL") {
    return "<input type='radio' name='acl_"+id+"' value='READ'> READ <input type='radio' name='acl_"+id+"' value='WRITE' > WRITE <input type='radio' name='acl_"+id+"' value='FULL_CONTROL' checked='checked'> FULL_CONTROL";
  }
}

function delMetaItem(ts){
  if (document.getElementById("upload_file_meta_list").getElementsByTagName("li").length==1) {
    document.getElementById("upload_file_set_meta_container").innerHTML="No Meta Items";
  }else{
    document.getElementById("upload_file_meta_list").removeChild(document.getElementById('upload_file_meta_item_'+ts));
  }
}

function showDeleteObjectsDiv() {
  buildOperTarget();
  $( "#delete_object_div" ).dialog("open"); ;
}

function loadBucketView() {
  var listView;
  if (bucketList.buckets.bucket.length == 0) {
    listView="You have no bucket."
  } else {
    listView = "<ul id='bucket_list' class='bucket_list'>";
    for (var i=0; i<bucketList.buckets.bucket.length; i++){
      listView+="<li id='bucket_"+i+"' class='bucket_item bucketMenu bucket_unselected'><img src='images/bucket.png' align='absmiddle' />"+ bucketList.buckets.bucket[i].name+"</li>";
    }
    listView+="</ul>";
  }
  $("#bucket_container").html(listView);
  loadBucketSelectEffect();
  loadBucketContextMenu();

  var loadIndex = -1;
  for (var i=0; i<bucketList.buckets.bucket.length; i++) {
    if (current_bucket == bucketList.buckets.bucket[i].name) {
      loadIndex = i;
      break;
    }
  }
  if (loadIndex == -1) {
    $("#object_tool_div").hide();
    $("#object_title_div").hide();
    $("#create_folder_div").hide();
    $("#object_container").hide();
  } else {
    $(document.getElementById("bucket_"+loadIndex)).click();
  }
}

function loadObjectView(){
  $("#object_tool_div").show();
  $("#object_title_div").show();
  $("#object_container").show();
	
  var listView;
  
  var fullPath = (typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
  var vFolders = bucketVFolder[current_bucket_index].vFolder.getFolders(fullPath);
  
  if (objectList.commonprefixes.length == 0 && objectList.contents.length == 0 && (vFolders == null || vFolders.length == 0)) {
    if (typeof objectList.prefix == "undefined") {
      listView = "The bucket has no objects.";
    } else {
      listView = "<ul id='object_list' class='object_list'>";
      listView+="<li id='parent_folder' class='object_item'><img src='images/folder.png' align='absmiddle' /> .. </li>";
	  listView+="</ul>";
    }
  } else {
    listView = "<ul id='object_list' class='object_list'>";
    if (typeof objectList.prefix != "undefined") {
      listView+="<li id='parent_folder' class='object_item'><img src='images/folder.png' align='absmiddle' /> .. </li>";
    }
    if (vFolders != null) {
      for (var i=0; i<vFolders.length; i++){
        var realFolder = null; 
        for (var j=0; j<objectList.commonprefixes.length; j++) {
          var prefixes = objectList.commonprefixes[j].prefix.split("/");
          if (prefixes[prefixes.length-2] == vFolders[i].name) {
            realFolder = objectList.commonprefixes[j];
            break;
          }
        }
        if (realFolder == null)
          listView+="<li id='v_folder_"+i+"' class='object_item objectMenu object_unselected'><img src='images/folder.png' align='absmiddle' /> "+ vFolders[i].name +"</li>";
      }
    }
    for (var i=0; i<objectList.commonprefixes.length; i++){
      var prefixes = objectList.commonprefixes[i].prefix.split("/");
      listView+="<li id='real_folder_"+i+"' class='object_item objectMenu object_unselected'><img src='images/folder.png' align='absmiddle' /> "+ prefixes[prefixes.length-2]+"</li>";
    }
    
    var nameLength = pageWidth-225-360-21;
    for (var i=0; i<objectList.contents.length; i++){
      var prefixes = objectList.contents[i].key.split("/");
      listView+="<li id='object_"+i+"' class='object_item objectMenu object_unselected'><span id='object_"+i+"_name'  class='object_item_name' style='width:"+nameLength+"px'><img src='images/object.png' align='absmiddle' /> "+ prefixes[prefixes.length-1]+"</span><span class='object_item_size'>"+objectList.contents[i].size+" bytes</span><span class='object_item_time'>"+objectList.contents[i].lastmodified+"</span></li>";
    }
    listView+="</ul>";
  }
  $("#object_container").html(listView);
  loadObjectSelectEffect();
  loadObjectContextMenu();
  loadObjectContextMenu2();
}

function createFolder(){
  var folderName = jQuery.trim($("#create_folder_name")[0].value);
  if (folderName=="")
    return;
  if (typeof objectList.commonprefixes != "undefined") {
    for (var i=0; i<objectList.commonprefixes.length; i++) {
      var prefixes = objectList.commonprefixes[i].prefix.split("/");
      if (prefixes[prefixes.length-2]==folderName) {
        $("#create_folder_div").hide("fade",500);
        return;
      }
    }
  }
  
  if (typeof objectList.contents != "undefined") {
    for (var i=0; i<objectList.contents.length; i++) {
      var prefixes = objectList.contents[i].key.split("/");
      if (prefixes[prefixes.length-1]==folderName) {
        $("#create_folder_div").hide("fade",500);
        return;
      }
    }
  }
  
  var fullPath = ((typeof objectList.prefix == "undefined")?"":objectList.prefix)+folderName;
  var vFolder = bucketVFolder[current_bucket_index].vFolder;
  if (!vFolder.exist(fullPath)) {
    vFolder.add(fullPath);
  }
  loadObjectView();
  $("#create_folder_div").hide("fade",500);
}

function buildOperTarget(){
  operTarget.folder=(typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
  operTarget.list = new Array();
  for (var i=0; i<selected_objects.length; i++) {
    if (selected_objects[i].indexOf("v_folder_")==0) {
      var index = Number(selected_objects[i].substr(9));
      var p = (typeof objectList.prefix == "undefined")?"":objectList.prefix.substr(0, objectList.prefix.length-1);
	  var path = ((typeof objectList.prefix == "undefined")?"":objectList.prefix)+bucketVFolder[current_bucket_index].vFolder.get(p).subFolders[index].name;
      var o = {
        path : path,
        real : false, 
      }; 
      operTarget.list.push(o);
    } else if (selected_objects[i].indexOf("object_") == 0) {
      var index = Number(selected_objects[i].substr(7));
      var o = {
        path : objectList.contents[index].key,
        real : true,
      }
      operTarget.list.push(o);
    } else if (selected_objects[i].indexOf("real_folder_") == 0) {
      var index = Number(selected_objects[i].substr(12));
      var o = {
        path : objectList.commonprefixes[index].prefix,
        real : true,
      }
      operTarget.list.push(o);
    }
  }
}

function buildOperObject(){
  operObject.folder=operTarget.folder;
  for (var i=0; i<operTarget.list.length; i++) {
    if (operTarget.list[i].real==false) {
      operObject.list.push(operTarget.list[i]);
    } else {
      if (operTarget.list[i].path.lastIndexOf("/") != (operTarget.list[i].path.length-1)) { // object
        operObject.list.push(operTarget.list[i]);
      } else {                                                                               // folder
        var reqString = buildRequest("GET",bucketList.buckets.bucket[current_bucket_index].name+"?prefix="+operTarget.list[i].path.substr(1));
        $.ajax({
          url: '/oper/RockStor.action',
          type: 'POST',
		  async: false,
          data: 'httpReq='+reqString,
          success: function(m){
            if (m.httpResp.status=="600"){
              return;
            }
            var xmlData=xml2json.parser(m.httpResp.body);
            if (m.httpResp.status!="200"){
              var error=xmlData.error;
              return;
            }
            var _objectList = xmlData.listbucketresult;
            if (typeof _objectList.contents == "undefined") {
              return;
            } else if(!(_objectList.contents instanceof Array)) {
              var tmp = _objectList.contents;
              _objectList.contents = new Array();
              _objectList.contents[0] = tmp;
            }
            for (var j=0; j<_objectList.contents.length; j++) {
              var o = {
                path : _objectList.contents[j].key,
                real : true,				
              }
              operObject.list.push(o);
            }
          },
          error: function(){
          }
        });
      }
    }
  }
}

function addActionItem(type, action, success) {
  var node = document.createElement("li");
  node.className = "bottom_action_item";
  var img = "images/";
  if (type=="CreateBucket") img+="create-bucket";
  else if (type=="Delete") img+="delete";
  else if (type=="Upload") img+="upload";
  else if (type=="Copy") img+="copy";
  else if (type=="Move") img+="cut";
  else if (type=="Rename") img+="rename";
  else if (type=="Acl") img+="acl";
  else img+="actions";
  img+=".png";
  var width = pageWidth-450;
  var view = "<span class='bottom_action_item_name' style='width:"+width+"px'><img src="+img+" align='absmiddle' /> "+action+"</span>";
  view += "<span class='bottom_action_item_time'>"+new Date()+"</span>";
  view += "<span class='bottom_action_item_result'>"+success+"</span>";
  node.innerHTML=view;
  var olds = document.getElementById("bottom_action_list").getElementsByTagName("li");
  if (olds.length == 0) {
    document.getElementById("bottom_action_list").appendChild(node);
  } else {
    document.getElementById("bottom_action_list").insertBefore(node,olds[0]);
  }
}

function clearActionList() {
  $("#bottom_action_list").html("");
}
