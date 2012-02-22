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

function VFolder(fname) {
  this.name = fname;
  this.subFolders = new Array();
  
  this.hasSub = function() {
    return (this.subFolders.length>0);
  };
  
  this.add = function(subFolder) {
    var index = subFolder.indexOf("/");
    if (index == -1) {
      var f = new VFolder(subFolder);
      this.subFolders.push(f);
	} else {
      dirFolder = subFolder.substring(0, index);
      subSubFolder = subFolder.substr(index+1);
	  var f = null;
      for (var i=0; i<this.subFolders.length; i++) {
        if (this.subFolders[i].name == dirFolder) {
			f = this.subFolders[i];
		}
      }
	  if (f==null) {
		  f = new VFolder(dirFolder);
        this.subFolders.push(f);
	  }

      f.add(subSubFolder);
    }
  };

  this.exist = function(subFolder) {
    var index = subFolder.indexOf("/");
	if (index == -1) {
      return (this.getDirectSub(subFolder) !=null)
	} else {
      var dirFolder = subFolder.substring(0, index);
      var subSubFolder = subFolder.substr(index+1);
      var s = this.getDirectSub(dirFolder); 
      return (s == null)?false:s.exist(subSubFolder);
    }
  };
  
  this.remove = function(subFolder) {
    if (subFolder=="") return false;  
    var index = subFolder.indexOf("/");
    if (index == -1) {
      var fi = -1;
      for (var i=0; i<this.subFolders.length; i++) {
        if (this.subFolders[i].name==subFolder) {
          fi = i;
          break;
        }
      }
      if (fi != -1) {
        this.subFolders.splice(fi, 1);
      }
    } else {
      var dirFolder = subFolder.substring(0, index);
      var subSubFolder = subFolder.substr(index+1);
      var s = this.getDirectSub(dirFolder);
      if (s!=null) {
        s.remove(subSubFolder)
	    if (s.subFolders.length==0) {
          var fi = -1;
          for (var i=0; i<this.subFolders.length; i++) {
            if (this.subFolders[i].name==dirFolder) {
              fi = i;
              break;
            }
          }
          if (fi != -1) {
            this.subFolders.splice(fi, 1);
          }
        }
      }
    }
  };

  this.getFolders = function(subFolder) {
    var index = subFolder.indexOf("/");
    if (index == -1) {
      if (subFolder=="") 
        return this.subFolders;
      var ds = this.getDirectSub(subFolder);
      if (ds != null) {
		  var ret = ds.getFolders("")
		  return ret;
      }
	  return null;
	} else {
      dirFolder = subFolder.substring(0, index);
      subSubFolder = subFolder.substr(index+1);
      var s = this.getDirectSub(dirFolder);
      return (s==null)?null:s.getFolders(subSubFolder);
    }
  };

  this.get = function(subFolder) {
    if (subFolder=="") return this;
    var index = subFolder.indexOf("/");
    if (index == -1) return this.getDirectSub(subFolder);
    dirFolder = subFolder.substring(0, index);
    subSubFolder = subFolder.substr(index+1);
    var s = this.getDirectSub(dirFolder);
    return (s==null)?null:s.get(subSubFolder);
  };

  this.getDirectSub = function(subFolder) {
    for (var i=0; i<this.subFolders.length; i++) {
      if (this.subFolders[i].name==subFolder) {
        return this.subFolders[i];
      }
    }
    return null;
  };
}
