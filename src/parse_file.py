import re
import io
from datetime import datetime, timedelta
import queue


mon_to_num = {}
for i in range(1,13):
    abbr = datetime.strptime(str(i),'%m').strftime('%b')
    mon_to_num[abbr] = i


def datetime_from_string(date_string, naive=True):
	"""Parses a date string in the form: '01/Jul/1995:00:00:01 -0400'
	into its component parts and returns equivalent datetime object."""
	d = int(date_string[0:2])
	mon = mon_to_num[date_string[3:6]]
	y = int(date_string[7:11])
	h = int(date_string[12:14])
	mn = int(date_string[15:17])
	s = int(date_string[18:20])
	return datetime(day=d,month=mon,year=y,hour=h,minute=mn,second=s)
           
def feature1(input_file):
	"""Returns a dict of hosts and the number of times
	they have accessed the site during the period of
	time covered by the input file"""
	host_dict = {}
	with open(input_file, 'r', errors='replace') as f:
		for line in f:
			host = line[:line.find(' ')]
			if host not in host_dict:
				host_dict[host] = 0
			host_dict[host] += 1
	return host_dict

def feature2(input_file):
	"""Returns a dict of resources and number of bytes
	consumed by each during the period of time covered
	by the input file"""
	resource_dict = {}
	#resource_regex = re.compile(r'.*\"(.+)\"\s+\d+\s+([\d+|-])')
	resource_regex = re.compile(r'.*["\u201c\u201d](.+)["\u201c\u201d]\s+\d+\s+([\d+|-])')

	with io.open (input_file, 'r',errors='replace') as f:
	    for line in f:
	        mo = resource_regex.search(line)
	        try:
	            bts = int(mo.group(2))
	        except ValueError:
	            bts = 0
	        resource = mo.group(1)
	        if resource not in resource_dict:        
	            resource_dict[resource] = 0
	        resource_dict[resource] += bts
	return resource_dict


def feature3(input_file):
	"""Returns a list of timestamps converted into datetime format"""
	timestamps = []
	cur_timestamp_string = None
	timestamp_regex = re.compile(r'.*\[(.*)\]\s["\u201c\u201d]')
	with open (input_file, 'r', errors='replace') as f:
	    for line in f:
	        mo = timestamp_regex.search(line)
	        timestamp_string = mo.group(1)
	        if timestamp_string != cur_timestamp_string:
	            cur_timestamp_string = timestamp_string
	            cur_timestamp = datetime_from_string(cur_timestamp_string)
	        timestamps.append(cur_timestamp)
	return timestamps


def parse_file(input_file, encoding='ISO-8859-1'):
	host_dict = {}
	resource_dict = {}
	timestamps = []
	cur_timestamp_string = None

	logfile_regex = re.compile(
		r'(\S+).*\[(.*)\]\s\"(.*)\"\s*(\d+)\s+([\d+|-])')
	
	with open(input_file, 'r', encoding = encoding) as f:
		for line in f:
			mo = logfile_regex.search(line)
			ip,timestamp_string,resource, response, bts = mo.groups()

			if ip not in host_dict:
				host_dict[ip] = 0
			host_dict[ip] += 1

			try:
				bts = int(bts)
			except ValueError:
				bts = 0
			if resource not in resource_dict:
				resource_dict[resource] = 0
			resource_dict[resource] += bts

			if timestamp_string != cur_timestamp_string:
				cur_timestamp_string = timestamp_string
				cur_timestamp = datetime_from_string(cur_timestamp_string)
			timestamps.append(cur_timestamp)		
	return host_dict, resource_dict, timestamps





def feature4(input_file, request='/login', blocked_mins = 5, failed_sec = 20, failed_threshold=3,
	encoding = 'ISO-8859-1'):
	"""Returns log of requests that would be blocked based on multiple failed requests during
	a specified time threshold"""
	blocked = {}
	blocked_log = []
	failed_attempt = {}
	logfile_regex = re.compile(r'(\S+).*\[(.*)\]\s+["\u201c\u201d](.*)["\u201c\u201d]\s+(\d+)')
	with open (input_file, 'r', errors='replace') as f:
	    for line in f:
	        mo = logfile_regex.search(line)
	        ip, timestamp, resource, result = mo.groups()
	        timestamp = datetime_from_string(timestamp)
	        
	        if ip in blocked:
	        #current request is from blocked host
	            if ((timestamp - blocked[ip]) <=
	                timedelta(minutes=blocked_mins)):
	            #still in blocked period.  Write to log
	                blocked_log.append(line)
	                continue
	            else: #blocked period has ended.  Remove item 
	                blocked.pop(ip, None)
	                
	        if request in resource:
	            if result == '200':
	            #successful login attempt nullifies failed attempts
	                failed_attempt.pop(ip,None)
	            else: #failed request
	                if ip not in failed_attempt:
	                #No previous failed requests.  Initialize
	                    failed_attempt[ip] = queue.Queue()
	                #clear old entries
	                while not failed_attempt[ip].is_empty():
	                    fail_time = failed_attempt[ip].head.value
	                    if (timestamp - fail_time) > timedelta(seconds=failed_sec):
	                        failed_attempt[ip].remove()
	                    else:
	                        break
	                failed_attempt[ip].insert(timestamp)
	                if failed_attempt[ip].length >= failed_threshold:
	                    blocked[ip] = timestamp
	return blocked_log
	                
                    
def group_timestamps(timestamps,minutes=60):
	"""Returns timestamps aggregated by specified time duration"""
	#Reference: 
	#http://stackoverflow.com/questions/10875702/how-would-you-group-timestamps-by-chunks-of-given-duration
	current = timestamps[0]
	count = 0
	group = []
	for t in timestamps:
		if (t - current) < timedelta(minutes=minutes):
			count = count + 1
		else:
			group.append((current,count))
			current = t
			count = 1
	group.append((current,count))
	return group




