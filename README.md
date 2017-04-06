# Fansite Analytics Challengs

The objective of this challenge is to perform analytics on a web server log file, provide useful metrics, and implement basic security measures.

### Dependencies

This program was written in Python 3.6.0.  Dependencies listed in `requirements.txt`.

### Feature 1: 
List the top 10 most active host/IP addresses that have accessed the site.

### Feature 2: 
Identify the 10 resources that consume the most bandwidth on the site

### Feature 3:
List the top 10 busiest (or most frequently visited) 60-minute periods 

### Feature 4: 
Detect patterns of three failed login attempts from the same IP address over 20 seconds so that all further attempts to the site can be blocked for 5 minutes. Log those possible security breaches.

### Notes:

* Even though it is resource intensive to read the log file more than once, the failed login attempt detection and IP blocking is a feature that would be implemented in against an input stream, so I've opted to implement the fourth feature separately from features 1-3.

* My assumption is that the timezone is a constant as it is part of the server's access timestamp.  I suppose, though, that ignoring the timezone could be a problem during the change to daylight savings time (fall back).

* The feature 3 implementation looks at each time period in the log and counts the number of entries that are within 60 minutes.  This creates a rolling 60 minute window over the span of the log.  I first took a different approach to this problem - grouping the timestamps into non-overlapping 60 minute periods. 


