#!/usr/bin/python
# Handling for Azure Media Services

import os
import sys

import urllib
import urlparse
import simplejson
import time
import datetime
import httplib

import collections
import pprint

##
# Config

# Configuration file for client. Contains key/value pairs, including at least ACCOUNT_NAME and ACCOUNT_KEY
AZURE_CONFIG = "azure.conf"

ENDPOINT = "https://media.windows.net/api/"

OAUTH_ENDPOINT = "https://wamsprodglobal001acs.accesscontrol.windows.net/v2/OAuth2-13"
OAUTH_SCOPE = "urn:WindowsAzureMediaServices"

HEADERS = {"x-ms-version": "2.2",
           "DataServiceVersion": "3.0",
           "MaxDataServiceVersion": "3.0",

           "Accept": "application/json",#;odata=verbose",
           }

PUTHEADERS = {"x-ms-version": "2012-02-12",
              "Accept": "application/json"}

##

class AzureMediaServices(object):

   def __init__(self, azure_config=None, account_name=None, account_key=None, debug=True):

      if azure_config:
         # Parse config file
         if not os.path.exists(azure_config):
            print "Error! Config file %s does not exist"%(azure_config,)
            print "It needs to contain something like the following:"
            print "ACCOUNT_NAME = <azure media services account name>"
            print "ACCOUNT_KEY = <azure media services account key>"
            raise SystemExit(1)

         for line in file(azure_config,"r").readlines():
            if "=" not in line:
               continue
            key,value = map(lambda x:x.strip(),line.split("=",1))

            if key.lower() == "account_name":
               self.account_name = value
            elif key.lower() == "account_key":
               self.account_key = value

         if not self.account_name or not self.account_key:
            print "Error! Failed to parse ACCOUNT_NAME and ACCOUNT_KEY from config file %s"%(azure_config,)
            raise SystemExit(1)

      elif account_name and account_key:
         # Alternatively pass in account name and key explicitly
         self.account_name = account_name
         self.account_key = account_key

      else:
         print "Error! Either azure_config filename or account_name and account_key need to provided"
         raise SystemExit(1)

      self.originalendpoint = ENDPOINT
      self.endpoint = ENDPOINT
      self.host = urlparse.urlsplit(ENDPOINT).netloc

      self.token = None
      self.token_expires = 0

      # How soon before expiry should re-authentication happen
      self.token_refresh = 1000

      # Very basic HTTPS connection caching
      self.conncache = {}

      self.debug = debug

   ##
   # Internal fetch methods

   def _GET(self, apiurl, urlargs=None, extraheaders=None):
      "Do a GET request for the given api URL + dictionary for URL arguments"

      self.checkauth()

      headers = HEADERS.copy()
      headers["Authorization"] = "Bearer "+self.token
      headers["Host"] = self.host
      if extraheaders:
         headers.update(extraheaders)

      url = self.endpoint + apiurl

      response, result = self._request("GET", url, urlargs=urlargs, headers=headers)
      if response.status == 301:
         self.endpoint = response.getheader("location")
         headers["Host"] = self.host = urlparse.urlsplit(self.endpoint).netloc
         url = self.endpoint + apiurl
         response, result = self._request("GET", url, urlargs=urlargs, headers=headers)

      if response.getheader("content-type","").startswith("application/json"):
         return response.status, simplejson.loads(result)
      return response.status, result

   def _POST(self, apiurl, data, extraheaders=None):
      "Internal method to handle POST of a given message. data is typically a dictionary"

      datastring = simplejson.dumps(data)

      self.checkauth()

      headers = HEADERS.copy()
      headers["Authorization"] = "Bearer "+self.token
      headers["Host"] = self.host
      headers["Content-type"] = "application/json"
      if extraheaders:
         headers.update(extraheaders)

      url = self.endpoint + apiurl

      response, result = self._request("POST", url, data=datastring, headers=headers)
      if response.status == 301:
         self.endpoint = response.getheader("location")
         headers["Host"] = self.host = urlparse.urlsplit(self.endpoint).netloc
         url = self.endpoint + apiurl
         response, result = self._request("POST", url, data=datastring, headers=headers)

      if response.getheader("content-type","").startswith("application/json"):
         return response.status, simplejson.loads(result)
      return response.status, result

   def _PUT(self, url, dataorfile, extraheaders=None, contenttype="application/octet-stream", quiet=False):
      "Do a put request to the given URL. Note this is a non-authed full URL, not an API one"

      headers = PUTHEADERS.copy()
      headers["Host"] = urlparse.urlsplit(url).netloc
      if extraheaders:
         headers.update(extraheaders)

      if type(dataorfile) is str:
         contentlength = None
         data = dataorfile
      else:
         contentlength = os.fstat(fileobj.fileno()).st_size
         def dataiter(chunksize=1024**2):
            fileobj.seek(0)
            while True:
               chunk = fileobj.read(chunksize)
               if not chunk:
                  break
               yield chunk
         data = dataiter()

      response, result = self._request("PUT", url, data=data, headers=headers, contenttype=contenttype, contentlength=contentlength, quiet=quiet)
      if response.getheader("content-type","").startswith("application/json"):
         return response.status, simplejson.loads(result)
      return response.status, result

   def _DELETE(self, apiurl, urlargs=None, extraheaders=None):
      "Do a DELETE request for the given api URL"

      self.checkauth()

      headers = HEADERS.copy()
      headers["Authorization"] = "Bearer "+self.token
      headers["Host"] = self.host
      if extraheaders:
         headers.update(extraheaders)

      url = self.endpoint + apiurl

      response, result = self._request("DELETE", url, urlargs=urlargs, headers=headers)
      if response.status == 301:
         self.endpoint = response.getheader("location")
         headers["Host"] = self.host = urlparse.urlsplit(self.endpoint).netloc
         url = self.endpoint + apiurl
         response, result = self._request("DELETE", url, urlargs=urlargs, headers=headers)

      if response.getheader("content-type","").startswith("application/json"):
         return response.status, simplejson.loads(result)
      return response.status, result


   ##
   # Utility functions for the above

   def _checkwrapper(self, wrapfunc, expectedcode, *args, **kw):
      code, result = wrapfunc(*args, **kw)
      if code != expectedcode:
         print "!!! Got unexpected code %d, expecting %d. Response follows:"%(code, expectedcode)
         pprint.pprint(result)
         raise SystemExit(5)
      return result

   def _checkGET(self, expectedcode, *args, **kw):    return self._checkwrapper(self._GET, expectedcode, *args, **kw)
   def _checkPOST(self, expectedcode, *args, **kw):   return self._checkwrapper(self._POST, expectedcode, *args, **kw)
   def _checkPUT(self, expectedcode, *args, **kw):    return self._checkwrapper(self._PUT, expectedcode, *args, **kw)
   def _checkDELETE(self, expectedcode, *args, **kw): return self._checkwrapper(self._DELETE, expectedcode, *args, **kw)

   ##

   def _request(self, method, url, urlargs=None, data=None, headers=None, contenttype="application/x-www-form-urlencoded", contentlength=None, quiet=False):
      "Do a http request"

      if headers is None:
         headers = {}

      if quiet:
         showdata = 10
      else:
         showdata = 1000

      headers.setdefault("Accept","text/plain")

      if data is not None:
         assert method in ("POST","PUT"),"Invalid to give data for %r method"%(method,)

         if callable(data):
            data = data()

         if type(data) is dict:
            data = urllib.urlencode(data)
         elif type(data) is str:
            pass # great!
         elif type(data) is unicode:
            data = data.encode("UTF-8")
         else:
            assert hasattr(data,"next"),"Invalid data provided to POST - got %r"%(type(data),)

         headers.setdefault("Content-type",contenttype)
         if contentlength:
            headers.setdefault("Content-Length",str(contentlength))
         else:
            headers.setdefault("Content-Length",str(len(data)))

      else:
         assert method not in ("POST","PUT"),"Invalid to not give data for %r method"%(method,)

      (scheme, netloc, path, query, _) = urlparse.urlsplit(url)
      assert scheme == "https","Invalid scheme %r"%(scheme,)
      if scheme.lower() == "https" and ":" not in netloc:
         netloc = netloc+":443"

      if urlargs:
         if query:
            path += "?" + query + "&" + urllib.urlencode(urlargs)
            url += "&" + urllib.urlencode(urlargs)
         else:
            path += "?" + urllib.urlencode(urlargs)
            url += "?" + urllib.urlencode(urlargs)
      elif query:
         path += "?" + query

      if self.debug:
         print >>sys.stderr,"+++",method,url

      starttime = time.time()

      # Build/Cache connection
      if self.conncache.has_key(netloc):
         conn = self.conncache[netloc]
      else:
         conn = httplib.HTTPSConnection(netloc)
         self.conncache[netloc] = conn
      conn.request(method, path, None, headers)

      if data:
         if not hasattr(data,"next"):
            conn.send(data)
            if self.debug:
               print >>sys.stderr,"+++ Sent %d bytes %d secs rate %dkbps starting %r"%(len(data),time.time()-starttime,len(data)/(time.time()-starttime+0.01)/1024,data[:showdata])
         else:
            # data as an iterator

            sentdata = 0
            printevery = 8 * 1024**2 # 8MB
            printnext = printevery

            iterout = ""
            for x in data:
               if len(iterout) < showdata:
                  iterout += x[:showdata-len(iterout)]
               conn.send(x)

               sentdata += len(x)
               if sentdata >= printnext and self.debug:
                  print >>sys.stderr,"++++ Sent %d bytes %d secs rate %dkbps"%(sentdata,time.time()-starttime,sentdata/(time.time()-starttime+0.01)/1024)
                  printnext += printevery
            if self.debug:
               print >>sys.stderr,"+++ Iterator data starting %r (%d bytes total)"%(iterout,sentdata)
               print >>sys.stderr,"++++ Sent %d bytes %d secs rate %dkbps"%(sentdata,time.time()-starttime,sentdata/(time.time()-starttime+0.01)/1024)

      response = conn.getresponse()
      resdata = response.read()

      endtime = time.time()
      if self.debug:
         print >>sys.stderr,"+++ Got %s response (%d bytes) in %.1fms"%(response.status, len(resdata), (endtime-starttime)*1000)

      return response, resdata

   ##
   # Authentication functions

   def auth(self):
      "Handle authentication for this endpoint"

      dataparams = {"grant_type": "client_credentials",
                    "client_id": self.account_name,
                    "client_secret": self.account_key,
                    "scope": OAUTH_SCOPE}

      data = urllib.urlencode(dataparams)
      headers = HEADERS.copy()

      response, result = self._request("POST", OAUTH_ENDPOINT, data=data, headers=headers)
      assert response.status==200, "Invalid response to login"
      auth_result = simplejson.loads(result)

      self.token = auth_result["access_token"]
      self.token_expires = time.time() + int(auth_result["expires_in"])

   def checkauth(self):
      "Do auth if required"
      if not self.token or time.time() > (self.token_expires - self.token_refresh):
         self.auth()

   ##

   def GetMediaProcessor(self, processorname, version=None):
      "Get the ID for a given media processor. Either return the given version (as a string), or the latest"

      result = self._checkGET(200, "MediaProcessors()", {"$filter": "Name eq '%s'"%(processorname,)})
      if len(result["value"]) == 0:
         raise Exception("Invalid/Missing MediaProcessor name %r"%(processorname,))

      processors = []
      gotversions = []
      for proc in result["value"]:
         if not version or version == proc["Version"]:
            processors.append((float(proc["Version"]),proc["Id"]))
         gotversions.append(proc["Version"])

      if len(processors) == 0:
         raise Exception("Invalid MediaProcessor version %r (for %r) - got versions %s"%(version, processorname, ",".join(gotversions)))

      # Get ID for highest version
      processors.sort()
      return processors[-1][1]

   def CheckPreviewMediaProcessor(self):
      "Check if Preview Media Processor is available"
      try:
         self.GetMediaProcessor("Media Encoder - Preview")
         return True
      except:
         return False

   ##
   # Access Policy

   def GetAccessPolicies(self):
      "Get information about Access Policies"
      return self._checkGET(200, "AccessPolicies")

   def AccessPolicy(self, name, durationinminutes, permissions):
      "Either create a new accesspolicy, or use an existing one if it exists with the right information"

      result = self._checkGET(200, "AccessPolicies")
      for accesspolicy in result["value"]:
         if (accesspolicy["Name"] == name and
             float(accesspolicy["DurationInMinutes"]) == float(durationinminutes) and
             int(accesspolicy["Permissions"]) == int(permissions)):
            return accesspolicy["Id"]

      result = self._checkPOST(201, "AccessPolicies", {"Name": name, "DurationInMinutes": str(durationinminutes), "Permissions": permissions})
      return result["Id"]

   ##
   # Blob/Block handling

   def UploadBlob(self, bloburl, fileobj, blocksize=4*1024**2):
      "Upload a Blob in Blocks (default to 4MB)"

      # Keep list of IDs for blocks (using 8-digit incrementing for now)
      blocks = []
      i = 0

      starttime = time.time()
      sentdata = 0

      fileobj.seek(0)
      while True:
         blockdata = fileobj.read(blocksize)
         if not blockdata:
            break
         blockid = ("%08d"%(i,)).encode("base64")

         self._checkPUT(201, bloburl + "&comp=block&blockid=" + urllib.quote(blockid), blockdata, quiet=True)

         blocks.append(blockid)
         i += 1

         sentdata += len(blockdata)

      blockxml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>"
      blockxml += "".join(["<Latest>%s</Latest>"%(b,) for b in blocks])
      blockxml += "</BlockList>"

      self._checkPUT(201, bloburl + "&comp=blocklist", blockxml, extraheaders={"Content-type": "text/plain; charset=UTF-8"})

      if self.debug:
         print "+++ Upload Blob: %d bytes %d secs rate %dkbps"%(sentdata,time.time()-starttime,sentdata/(time.time()-starttime+0.01)/1024)

   ##
   # Asset handling

   def UploadAsset(self, filename, name):
      "Upload a given asset from a local filename"

      # Create new asset
      result = self._checkPOST(201, "Assets", {"Name": name})
      assetid = result["Id"]

      # Get access policy (5 hours, Write-only access)
      accesspolicyid = self.AccessPolicy("UploadAccessPolicy", 300, 2)

      # As per docs, this is 5 minutes ago
      starttime = (datetime.datetime.utcnow() - datetime.timedelta(minutes=5)).isoformat()

      # SAS type = 1, OnDemandOrigin = 2
      locatortype = 1

      # Create locator
      locator = self._checkPOST(201, "Locators", {"AccessPolicyId": accesspolicyid, "AssetId": assetid, "StartTime": starttime, "Type": locatortype})
      locatorid = locator["Id"]
      locatorbaseuri = locator["BaseUri"]
      locationcontentaccesscomponent = locator["ContentAccessComponent"]

      # Add this filename to the given path
      uploadurl = urlparse.urljoin(locatorbaseuri.rstrip("/")+"/", os.path.basename(filename)) + locationcontentaccesscomponent

      # Upload as set of blocks
      fileobj = file(filename,"rb")
      self.UploadBlob(uploadurl, fileobj)

      # Remove locator
      self._checkDELETE(204, "Locators('%s')"%(locatorid,))

      # Create file infos
      self._checkGET(204, "CreateFileInfos", {"assetid": "'"+assetid+"'"})

      return assetid

   def CheckAsset(self, assetid):
      "Show information about given asset ID"

      result = self._checkGET(200, "Assets('%s')"%(urllib.quote(assetid),))
      print "Got result:"
      pprint.pprint(result)

   def ProcessAsset(self, inputassetid, name, processorname = "Windows Azure Media Encoder", taskpreset = "H264 Smooth Streaming 720p"):
      """
      Request a given asset is processed by the given processor/preset

      Processors: http://msdn.microsoft.com/en-us/library/windowsazure/jj129580.aspx
      Presets: http://msdn.microsoft.com/en-us/library/jj129582.aspx
      """

      # Get latest media processor for this name
      mediaprocessorid = self.GetMediaProcessor(processorname)

      jobname = "%s to %s"%(name,taskpreset)
      # Note this is a different format to normal (requires odata=verbose to be set)
      inputasseturi = self.originalendpoint + "Assets('%s')"%(urllib.quote(inputassetid),)
      taskbody = ("<?xml version=\"1.0\" encoding=\"utf-8\"?><taskBody><inputAsset>JobInputAsset(0)</inputAsset>"
                  "<outputAsset assetName=\"%s\">JobOutputAsset(0)</outputAsset></taskBody>"%("%s - %s"%(name,taskpreset),))

      task = {"Name": jobname,
              "InputMediaAssets": [{"__metadata": {"uri": inputasseturi}}],
              "Tasks": [{"Configuration": taskpreset, "MediaProcessorId": mediaprocessorid, "TaskBody": taskbody}],
              }

      result = self._checkPOST(201, "Jobs", task, extraheaders={"Content-type": "application/json;odata=verbose"})
      jobid = result["Id"]

      return jobid

   def JobState(self, jobid):
      """
      Return current job state, which will be one of:

      Queued = 0,
      Scheduled = 1,
      Processing = 2,
      Finished = 3,
      Error = 4,
      Canceled = 5, # (Note: Typo from docs)
      Canceling = 6
      """

      result = self._checkGET(200, "Jobs('%s')/State"%(urllib.quote(jobid),))
      return result["value"]

   def OutputMediaAssetID(self, jobid):
      "Get output asset ID for the given job"

      result = self._checkGET(200, "Jobs('%s')/OutputMediaAssets"%(urllib.quote(jobid),))
      return result["value"][0]["Id"]

   def PublishAsset(self, assetid):
      "Make this asset public (from now, and for the next year)"

      # Get access policy (5 hours, Write-only access)
      accesspolicyid = self.AccessPolicy("ViewerAccessPolicy", 60*24*365, 1)

      # As per docs, this is 5 minutes ago
      starttime = (datetime.datetime.utcnow() - datetime.timedelta(minutes=5)).isoformat()

      # SAS type = 1, OnDemandOrigin = 2
      locatortype = 2

      # Create locator and return ID
      locator = self._checkPOST(201, "Locators", {"AccessPolicyId": accesspolicyid, "AssetId": assetid, "StartTime": starttime, "Type": locatortype})
      return locator["Id"]

   def CheckLocator(self, locatorid):
      "Show details about a given locator"
      result = self._checkGET(200, "Locators('%s')"%(urllib.quote(locatorid),), extraheaders={"Accept": "application/json;odata=verbose"})
      print "Got result:"
      pprint.pprint(result)

   def GetPrimaryFile(self, assetid):
      "Get the primary filename for a given (output) asset ID. This is typically the basename for the smooth stream"

      result = self._checkGET(200, "Assets('%s')/Files"%(urllib.quote(assetid),))
      for output in result["value"]:
         if output["IsPrimary"]:
            return output["Name"]
      raise Exception("Unable to locate primary file")

   def GetOutputURL(self, locatorid, manifest="Manifest"):
      "Get an output URL to a smooth stream manifest file, for a given output locator ID"

      result = self._checkGET(200, "Locators('%s')"%(urllib.quote(locatorid),))

      assetid = result["AssetId"]
      basepath = result["Path"]
      filename = self.GetPrimaryFile(assetid)

      return basepath + filename + "/" + manifest

##
# Functions

def uploadandpublishfile(filename, visiblename):
   "Upload the given file and process with a suitable preset"

   ams = AzureMediaServices(AZURE_CONFIG)

   print "Uploading file %s (with name %r)"%(filename, visiblename)
   assetid = ams.UploadAsset(filename, visiblename)
   print "Uploaded file. Asset ID is %s"%(assetid,)
   jobid = ams.ProcessAsset(assetid, visiblename)
   print "Job created with ID %s. Now waiting until complete..."%(jobid,)
   outputassetid = ams.OutputMediaAssetID(jobid)
   print "Output asset ID is %s"%(outputassetid,)

   # Wait until job is complete
   statenames = {0:"Queued", 1:"Scheduled", 2:"Processing", 3:"Finished", 4:"Error", 5:"Cancelled", 6:"Cancelling"}
   state = None
   while True:
      state = ams.JobState(jobid)
      if state >= 3: # Finished/Error/Canceled/Canceling
         break
      print "Job %s in state %d (%s). Sleeping 10s and checking again"%(jobid, state, statenames.get(state,"Unknown"))
      time.sleep(10)

   if state != 3:
      print "Job %s in invalid state %d (%s). Aborting"%(jobid, state, statenames.get(state,"Unknown"))
      raise SystemExit(3)

   print "Job %s complete. Creating output locator"%(jobid,)

   outputlocatorid = ams.PublishAsset(outputassetid)
   outputurl = ams.GetOutputURL(outputlocatorid)
   print "Output locator ID is %s, and URL is %s"%(outputlocatorid, outputurl)

   print "Done"

##

if __name__ == "__main__":

   args = sys.argv[1:]

   if not args:
      print "Utility functions for handling Azure Content"
      print "Usage: %s upload <filename> <output name>"%(sys.argv[0],)
      print "          checkprocessor - see if various media processors are installed on this account"
      raise SystemExit(1)

   action = args[0]

   if action == "upload":
      uploadandpublishfile(args[1], args[2])

   elif action == "checkprocessor":

      ams = AzureMediaServices(AZURE_CONFIG)
      for x in ("Windows Azure Media Encoder", "Media Encoder - Preview"):
         print "Got encoder %r with ID %r"%(x, ams.GetMediaProcessor(x))

   else:
      print "Invalid action %s"%(action,)

##

