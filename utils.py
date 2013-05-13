# Copyright 2012 Alex Breshears.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import config
import urllib2
import simplejson as json
from urllib2 import HTTPError

# TODO: HTTPFS - Configure security

class HadoopHTTPFSException(Exception):
	def __init__(self, message, path):
		Exception.__init__(self, message)
		self.path = path

def ListDirectory(path='/'):
	"""
		Lists a directory in HDFS using HTTP-FS.
		See http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#LISTSTATUS

		path = path in HDFS
	"""
	if config.HTTPFS_USERNAME:
		user_for_url = '&user.name=' + config.HTTPFS_USERNAME
	else:
		user_for_url = ''
	url = config.HTTPFS_ROOT_URL + path + '?op=LISTSTATUS' + user_for_url
	request = urllib2.Request(url)
	response = urllib2.urlopen(request)
	if response.getcode() == 404:
		raise HadoopHTTPFSException('Directory not found.', path)
	json_response = json.loads(response)
	if 'RemoteException' in json_response:
		message = json_response['RemoteException'].message
		raise HadoopHTTPFSException(message=message, path=path)
	return json_response

def OpenFile(path):
	"""
		Open a file in HDFS using HTTP-FS
		See http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#OPEN

		path = path in HDFS
	"""
	if config.HTTPFS_USERNAME:
		user_for_url = '&user.name=' + config.HTTPFS_USERNAME
	else:
		user_for_url = ''
	url = config.HTTPFS_ROOT_URL + path + '?op=OPEN' + user_for_url
	request = urllib2.Request(url)
	response = urllib2.urlopen(request)
	if response.getcode() == 404:
		raise HadoopHTTPFSException('File not found.', path)
	file = response.read()
	json_response = None
	try:
		json_response = json.loads(file)
	except Exception:
		pass
	if json_response:
		if 'RemoteException' in json_response:
			message = json_response['RemoteException'].message
			raise HadoopHTTPFSException(message=message, path=path)
	return file

def DeleteDirectory(path, recursive=False):
	"""
		Deletes a directory in HDFS using HTTP-FS
		See http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#DELETE

		path = path in HDFS
		recursive = recursively delete in the directory in HDFS
	"""
	if config.HTTPFS_USERNAME:
		user_for_url = '&user.name=' + config.HTTPFS_USERNAME
	else:
		user_for_url = ''
	if recursive:
		recursive_for_url = '&recursive=true'
	else:
		recursive_for_url = '&recursive=false'
	url = config.HTTPFS_ROOT_URL + path + '?op=DELETE' + user_for_url + recursive_for_url
	request = urllib2.Request(url)
	request.get_method = lambda: 'DELETE'
	response = urllib2.urlopen(request)
	file = response.read()
	json_response = None
	try:
		json_response = json.loads(file)
		if not json_response['boolean']:
			raise HadoopHTTPFSException('Directory does not exist.', path)
	except Exception:
		pass
	if json_response:
		if 'RemoteException' in json_response:
			message = json_response['RemoteException'].message
			raise HadoopHTTPFSException(message=message, path=path)
	return json_response

def DeleteFile(path):
	"""
		Deletes a file or a directory in HDFS using HTTP-FS
		See http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#DELETE

		path = path in HDFS
	"""
	if config.HTTPFS_USERNAME:
		user_for_url = '&user.name=' + config.HTTPFS_USERNAME
	else:
		user_for_url = ''
	url = config.HTTPFS_ROOT_URL + path + '?op=DELETE' + user_for_url
	request = urllib2.Request(url)
	request.get_method = lambda: 'DELETE'
	response = urllib2.urlopen(request)
	file = response.read()
	json_response = None
	try:
		json_response = json.loads(file)
		if not json_response['boolean']:
			raise HadoopHTTPFSException('File does not exist.', path)
	except Exception:
		pass
	if json_response:
		if 'RemoteException' in json_response:
			message = json_response['RemoteException'].message
			raise HadoopHTTPFSException(message=message, path=path)
	return json_response

def CreateDirectory(path, octal_permissions=None):
	"""
		Creates a directory in HDFS using HTTP-FS
		See http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#MKDIRS

		path = path in HDFS
		octal_permissions = octal permissions for the directory in HDFS
	"""
	if config.HTTPFS_USERNAME:
		user_for_url = '&user.name=' + config.HTTPFS_USERNAME
	else:
		user_for_url = ''
	if octal_permissions:
		octal_permissions_for_url = '&permission=' + octal_permissions
	else:
		octal_permissions_for_url = ''
	url = config.HTTPFS_ROOT_URL + path + '?op=MKDIRS' + user_for_url + octal_permissions_for_url
	request = urllib2.Request(url)
	request.get_method = lambda: 'PUT'
	response = urllib2.urlopen(request)
	file = response.read()
	json_response = None
	try:
		json_response = json.loads(file)
	except Exception:
		pass
	if json_response:
		if 'RemoteException' in json_response:
			message = json_response['RemoteException'].message
			raise HadoopHTTPFSException(message=message, path=path)
	return json_response

def CreateFile(path, content, url=None, redirect_count=0, overwrite=False, octal_permissions=None):
	"""
		Creates a file in HDFS using HTTP-FS
		See http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#CREATE

		path = path in HDFS
		content = file to write
		mime_type = media type for write http://en.wikipedia.org/wiki/Internet_media_type
		overwrite = overwrite exisiting file in HDFS
		octal_permissions = octal permissions for the directory in HDFS
	"""
	if config.HTTPFS_USERNAME:
		user_for_url = '&user.name=' + config.HTTPFS_USERNAME
	else:
		user_for_url = ''
	if octal_permissions:
		octal_permissions_for_url = '&permission=' + octal_permissions
	else:
		octal_permissions_for_url = ''
	if overwrite:
		overwrite_for_url = '&overwrite=true'
	else:
		overwrite_for_url = ''
	if not url:
		url_to_put = config.HTTPFS_ROOT_URL + path + '?op=CREATE' + user_for_url + octal_permissions_for_url + overwrite_for_url
	else:
		url_to_put = url
	if config.DEBUG:
		print("httpFS: Creating file at " + path + " PUT: " + url_to_put)
	request = urllib2.Request(url_to_put, data=content)
	request.add_header('content-type', 'application/octet-stream')
	request.get_method = lambda: 'PUT'
	try:
		response = urllib2.urlopen(request)
		file = response.read()
		json_response = None
		try:
			json_response = json.loads(file)
		except Exception:
			pass
		if json_response:
			if 'RemoteException' in json_response:
				message = json_response['RemoteException'].message
				raise HadoopHTTPFSException(message=message, path=path)
		return json_response
	except HTTPError, e:
		# TODO: Handle more HTTP Errors
		if e.code == 307:
			# There's a weird bug in urllib that won't follow the redirects. Since httpFS redirects to the data node to
			# write the file, we must follow the redirect.
			redirect_count += 1
			if config.MAX_REDIRECTS < redirect_count:
				raise HadoopHTTPFSException("Too many redirects.", path)
			if 'location' not in e.headers:
				raise HadoopHTTPFSException("There is no location to redirect to.", path)
			if config.DEBUG: print("httpFS: Redirecting to " + e.headers['location'])
			CreateFile(path, content, url=e.headers['location'], redirect_count=redirect_count, overwrite=overwrite,
					   octal_permissions=octal_permissions)
		elif e.code == 201:
			# Not an error. This is the expected 
			pass
		else:
			raise  HadoopHTTPFSException("There was an error creating the file.", path)