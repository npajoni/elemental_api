import httplib2
import socket
from xml.parsers.expat import ExpatError
import urlparse 
import os
from xml.etree.ElementTree import *
import sys
import time

class ElementalError(Exception):
    def __init__(self, value, critical=False):
	self.value = value
	self.critical = critical
	
    def __str__(self):
	return str(self.value)

    def __repr__(self):
	return self.value
	


def GetJobList(server):
    jobList = []
    resp, cont = server.Get()
    xml = fromstring(cont)
    for job in xml.findall('job'):
	eJob = ElementalJob(server, job.get('href').split('/')[2])
	jobList.append(eJob)
			
    return jobList
    

class ElementalJob(object):
    def __init__(self, server=None, id=None, xml=None):
	self.id			= id
	self.server		= server
	self.file_input		= ''
	self.files_output	= []
	self.profileId		= ''
	self.status		= ''
	self.priority		= ''
	self.progress		= ''
	self.average_fps	= ''
	self.elapsed		= ''
	self.error_messages	= []
	
	if xml is not None:
	    self.__LoadData(xml)
		
    
    def __LoadData(self, Xml = None):
#	print Xml
	if Xml != None:
	    try:
		JobXml = fromstring(Xml)
	    except ExpatError as err:
		raise ElementalError('Unable to load XML: %s' % err)
	    
	    self.id		= JobXml.get('href').split('/')[2]
	    self.status 	= JobXml.find('status').text
	    self.priority	= JobXml.find('priority').text
	    self.progress	= JobXml.find('pct_complete').text
	    self.average_fps	= JobXml.find('average_fps').text
	    self.elapsed	= JobXml.find('elapsed').text
	    self.file_input	= JobXml.find('input').find('file_input').find('uri').text
    	    self.files_output = []
    	    self.error_messages = []
    	    # Falta agregar al listado de outputs el m3u8 principal
		
	    
	    for output in JobXml.find('output_group').findall('output'):
	        uri = output.find('full_uri')
	        if uri != None:
    	    	    self.files_output.append(uri.text)
	    
	    if self.status == 'error':
	        for error in JobXml.find('error_messages').findall('error'):
	    	    self.error_messages.append(error.find('message').text)
	    	        
	return self
	
	
	
    def __Update(self):
	try:
	    content = self.server.Get(self.id)
	    self.__LoadData(content)
	except ElementalError as e:
	    raise ElementalError('Unable to update: %s' % e)
	


    def sendJob(self, server = None, Xml = None, priority = None):
	if Xml != None:
	    if server != None:
		try:
		    xmlJob = server.Post('','',Xml)
		    self.server = server
		    self.__LoadData(xmlJob)
		except ElementalError as e:
		    raise ElementalError('Unable to send job: %s' % e)
	    else:
		raise ElementalError('Elemental Server not specified')
	else:
	    raise ElementalError('Xml not specified')
	if priority != None:
	    try:
		self.setJobPriority(priority)	    
	    except:
		pass	
		
		
    def getJobStatus(self, update = False):
	if update == True:
	    self.__Update()
	
	return self.status
	    
    	    
    def getJobProgress(self, update = False):
	if update == True:
	    self.__Update()
	
	return self.progress
	
	
    def getJobPriority(self, update = False):
	if update == True:
	    self.__Update()
	
	return self.priority	
	
	
    def getJobElapsed(self, update = False):
	if update == True:
	    self.__Update()
	
	return self.elapsed
	
	
    def getJobAvgFps(self, update = False):
	if update == True:
	    self.__Update()
	
	return self.average_fps
	
    def getOutputFiles(self, update = False):
	if update == True:
	    self.__Update()
	
	return self.files_output
	
    	
    def loadJob(self):
	r,c = self.server.Get(self.id)
	print c		
			
    
    def setJobPriority(self, priority = None):
	xml = '<priority>' + str(priority) + '</priority>'
	try:
	    self.server.Post(self.id, 'priority', xml)
	except ElementalError as err:
	     raise ElementalError('Unable to set priority: %s' % err)
	    
    def cancelJob(self):
	xml = '<cancel></cancel>'
	try:
	    self.server.Post(self.id, 'cancel', xml)
	except ElementalError as err:
	     raise ElementalError('Unable to cancel job: %s' % err)

#    def resubmit(self):




class ElementalProfile(object):
    def __init__(self, id = ''):
	self.Id = id
	self.OutputOrder = '1'
	self.OutputGroupName = ''


class Elemental(object):
    def __init__(self, server = ''):
	self.server	= server
	self.baseuri    = 'http://%s' % self.server
	self.baseapi	= '/api/jobs'

    
    def Get(self, JobId = None, Command = None):
	method	= 'GET'
	body 	= ''
	header	=  { 'Accept' : 'application/xml' }
	
	if JobId is None:
	    uri = urlparse.urlparse(self.baseuri + self.baseapi) 
	else:
	    if Command is None:
		uri = urlparse.urlparse(self.baseuri + self.baseapi + '/' + JobId)
	    else:
		uri = urlparse.urlparse(self.baseuri + self.baseapi + '/' + JobId + '/' + Command)
	
	h = httplib2.Http()
		
	try:
	    response, content = h.request(uri.geturl(), method, body, header)
	except socket.error as err:
	    raise ElementalError(err)
	    
#	print response
#	print content
	if response['status'] == '200':
	    return content
	elif response['status'] == '404':
	    error = fromstring(content).find('error').text
	    raise ElementalError(error)
	
	#return ret


    def Post(self, JobId = None, Command = None, Xml = None):
	method = 'POST'
	header = { 'Accept' : 'application/xml',  'Content-type': 'application/xml' }
	
	if JobId is None:
	    uri = urlparse.urlparse(self.baseuri + self.baseapi)
	else:
	    uri = urlparse.urlparse(self.baseuri + self.baseapi + '/' + JobId + '/' + Command)

	h = httplib2.Http()
	
	try:
	    response, content = h.request(uri.geturl(),method,Xml,header)
	except socket.error as err:
	    raise ElementalError(err)
	    
#	print response
#	print content
	if response['status'] == '201':
	    return content
	elif response['status'] == '422':
	    error = fromstring(content).find('error').text
	    raise ElementalError(error)
	elif response['status'] == '404':
	    error = fromstring(content).find('error').text
	    raise ElementalError(error)
	    
	#return ret
	
		 

def CreateJobFromProfile(Server = None, Input= None, OutputPath = None, OutputFilename = None ,ElementalProfile =  None):
    
    if OutputPath != None:
	if OutputFilename != None:
	    if OutputPath.endswith('/'):
		Output = OutputPath + OutputFilename
	    else:
		Output = OutputPath + '/' + OutputFilename
	    if Input != None:
		if Server != None:
		    if ElementalProfile != None:
			xmlheader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
    
		        job = Element("job")
		        input = SubElement(job, "input")
		        file_input = SubElement(input, "file_input")
		        uri = SubElement(file_input, "uri")
		        uri.text = Input
		        output_group = SubElement(job, "output_group")
		        order = SubElement(output_group, "order")
		        order.text = ElementalProfile.OutputOrder
		        name = SubElement(output_group, "name")
		        name.attrib["nil"] = 'true'
		        group_settings =  SubElement(output_group, ElementalProfile.OutputGroupName)
		        destination = SubElement (group_settings, "destination")
		        uri = SubElement(destination, "uri")
		        uri.text = Output
		        profile = SubElement(job, "profile")
		        profile.text = ElementalProfile.Id

			#Test de Subs
		        caption_selector = SubElement(input, "caption_selector")
		        order = SubElement(caption_selector, "order")
		        order.text = "1"
		        source_type = SubElement(caption_selector, "source_type")
		        source_type.text = "SRT"
		        file_source_settings = SubElement(caption_selector, "file_source_settings")
		        infer_external_filename = SubElement(file_source_settings, "infer_external_filename")
		        infer_external_filename.text = "false"
		        time_delta = SubElement(file_source_settings, "time_delta")
		        time_delta.attrib['nil'] = 'true'
		        upconvert_608_to_708 = SubElement(file_source_settings, "upconvert_608_to_708")
		        source_file = SubElement(file_source_settings, "source_file")
		        uri = SubElement(source_file, "uri")
		        uri.text = "/data/mnt/input/Avengers.Age.of.Ultron.2015.1080p.BluRay.x264-SPARKS.srt"
		        
    
		        xml = xmlheader + tostring(job, encoding="utf-8")
			print xml
		        
		        eJob = ElementalJob(Server)
		        try:
			    eJob.sendJob(Server,xml)
			    return eJob
			except ElementalError as err:
			    raise ElementalError(err)    		        
		    else:
			raise ElementalError('Invalid Elemental Profile')
		else:
		    raise ElementalError('Invalid server')
	    else:
		raise ElementalError('Invalid input')
	else:
	    raise ElementalError('Invalid output filename')
    else:    
	raise ElementalError('Invalid output path')
   

def CreateJob(Server = None, Input= None, OutputPath = None, OutputFilename = None ,Preset =  None):

    if OutputPath != None:
        if OutputFilename != None:
            if OutputPath.endswith('/'):
                Output = OutputPath + OutputFilename
            else:
                Output = OutputPath + '/' + OutputFilename
            if Input != None:
                if Server != None:
                    if Preset != None:
                        xmlheader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"

                        job = Element("job")
                        input = SubElement(job, "input")
                        file_input = SubElement(input, "file_input")
                        uri = SubElement(file_input, "uri")
                        uri.text = Input
                        output_group = SubElement(job, "output_group")
                        order = SubElement(output_group, "order")
                        order.text = "1"
                        group_settings =  SubElement(output_group, "file_group_settings")
                        destination = SubElement (group_settings, "destination")
                        uri = SubElement(destination, "uri")
                        uri.text = Output
			type = SubElement(output_group, "type")
			type.text = "file_group_settings"
			output = SubElement (output_group, "output")
			stream_assembly_name =  SubElement (output, "stream_assembly_name")
			stream_assembly_name.text = "stream_1"
#			name_modifier = SubElement (output, "name_modifier")
#			name_modifier.text = "_test"
#			container = SubElement (output, "container")
#			container.text = "MPEG-4 Container"
			order = SubElement (output, "order")
			order.text = "1"
			preset = SubElement (output, "preset")
			preset.text = Preset
			stream_assembly = SubElement (job, "stream_assembly")
			name = SubElement (stream_assembly, "name")
			name.text = "stream_1"
			preset = SubElement (stream_assembly, "preset")
			preset.text = Preset

                        xml = xmlheader + tostring(job, encoding="utf-8")
                        print xml

                        eJob = ElementalJob(Server)
                        try:
                            eJob.sendJob(Server,xml)
                            return eJob
                        except ElementalError as err:
                            raise ElementalError(err)
                    else:
                        raise ElementalError('Invalid Preset')
                else:
                    raise ElementalError('Invalid server')
            else:
                raise ElementalError('Invalid input')
        else:
            raise ElementalError('Invalid output filename')
    else:
        raise ElementalError('Invalid output path')

