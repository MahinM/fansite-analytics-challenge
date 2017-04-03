import sys, getopt
import parse_file
import argparse

def output_results(output_file,results):
	"""Writes results to file"""
	with open(output_file,'w') as f:
		for line in results:
			print(line, file=f)


def feature1(input_file, output_file):
	"""Process input_file and write output to output_file
	for top 10 hosts"""
	host_dict = parse_file.feature1(input_file)
	top_hosts = sorted(host_dict.items(), key=lambda x: x[1],
		reverse = True)[:10]

	result = [str(item[0] + ',' + str(item[1])) for item in top_hosts]
	output_results(output_file, result)

def feature2(input_file, output_file):
	"""Process input_file and write output to output_file
	for top 10 resources requested"""
	resource_dict = parse_file.feature2(input_file)
	top_resources = sorted(resource_dict.items(), key=lambda x: x[1],
		reverse=True)[:10]

	result = [resource[0].split(' ')[1] for resource in top_resources]
	output_results(output_file, result)


def feature3(input_file, output_file,time_format):
	"""Process input_file and write output to output_file
	for top 10 hours of activity"""
	timestamps = parse_file.feature3(input_file)

	hours = parse_file.group_timestamps(timestamps)

	sorted_hours = sorted(hours, key=lambda x: x[1],
		reverse = True)[:10]

	result = [item[0].strftime(time_format) + ',' + str(item[1]) 
				for item in sorted_hours]
	output_results(output_file, result)



def feature4(input_file, output_file):
	"""Process input_file and write output to output_file
	for requests that would be blocked based on activity
	levels over threshold"""
	blocked_log = parse_file.feature4(input_file)

	result = [item.strip() for item in blocked_log]
	output_results(output_file, result)


def main():

	parser = argparse.ArgumentParser()
	parser.add_argument("input_file", help="path to log input file")
	parser.add_argument("host_output", help="feature 1: path to host output file")
	parser.add_argument("resources_output", help="feature 2: path to resources output file")
	parser.add_argument("hours_output", help="feature 3: path to hours output file")
	parser.add_argument("blocked_output", help="feature 4: path to blocked output file")
	args = parser.parse_args()


	time_format_naive = "%d/%b/%Y:%H:%M:%S"

	feature1(args.input_file, args.host_output)
	feature2(args.input_file, args.resources_output)
	feature3(args.input_file, args.hours_output, time_format_naive)
	feature4(args.input_file, args.blocked_output)
	


if __name__ == "__main__":
	main()