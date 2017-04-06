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
           

def log_summary(input_file, time_threshold=60):
	"""Processes the input log file and returns summary data structures for 3 features:
	host_dict: a dictionary with host/ip and count of the number of times the site has
		been accessed by that host.
	resource_dict: a dictionary with resource and count of the number of times the
		resource has been requested.
	time_totals_dict: a dictionary with timestamp and count of the number of entries
		that are within the input time_threshold of that time.
	"""
	host_dict = {}
	resource_dict = {}
	timestamps = []
	counts = []
	cur_timestamp_string = None
	logfile_regex = re.compile(r'(\S+).*\[(.*)\]\s+["\u201c\u201d](.*)["\u201c\u201d]\s+(\d+)\s+([\d+|-])')

	with open (input_file, 'r', errors='replace') as f:
		for line in f:
			mo = logfile_regex.search(line)
			host,timestamp_string,resource, response, bts = mo.groups()

			#feature 1
			if host not in host_dict:
				host_dict[host] = 0
			host_dict[host] += 1

			#feature 2
			try:
				bts = int(bts)
			except ValueError:
				bts = 0
			if resource not in resource_dict:        
				resource_dict[resource] = 0
			resource_dict[resource] += bts

	        #feature 3
			if timestamp_string != cur_timestamp_string:
				cur_timestamp_string = timestamp_string
				cur_timestamp = datetime_from_string(cur_timestamp_string)
				timestamps.append(cur_timestamp)
				counts.append(1)
			else: #add 1 to current count
				counts[-1] += 1

		#Complete processing of feature 3
	    #Get cumulative sum of timestamps for each starting point.
		total = 0
		time_totals_dict = {}
		next_index = 0
		for begin_index in range(0,len(timestamps)):
		#loop through each each unique time point...
			while (next_index < len(timestamps)): 
			#The trick here is that we just need to look at each subsequent time once. 
				if (timestamps[next_index]-timestamps[begin_index] < timedelta(minutes=time_threshold)):
	        	#if within 1 hour, increment count
					total += counts[next_index]
					next_index+=1
				else:
					break
			time_totals_dict[timestamps[begin_index]] = total
		    #About to increment the loop to look at the next time - remove previous count
			total = total - counts[begin_index]

	return host_dict, resource_dict, time_totals_dict




def failed_attempts(input_file, request='/login', blocked_mins = 5, failed_sec = 20, failed_threshold=3):
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


def feature3_non_overlapping(input_file):
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
