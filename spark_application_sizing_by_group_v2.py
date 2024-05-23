import base64
import csv
import json
import math
import requests
import sys

from datetime import datetime, timedelta

config = {}
users = {}

application_types = ['SPARK']

# Read all configuration from config file into a dictionary
with open(sys.argv[1]) as configfile:
    for line in configfile.readlines():
        key, value = line.rstrip("\n").split('=')
        config[key] = value

# Read password from a property file
with open(config['pfile']) as pfile:
    passwd = base64.b64decode(pfile.read().rstrip()).decode("utf-8")

analyzed_applications = []
skipped_applications = []
resource_events = []

for application_type in application_types:
    offset = 0
    analyzed_applications_count = 0
    skipped_applications_count = 0

    while True:
        response = requests.get(config['cm_url'] + '/api/v33/clusters/' + config['cluster_name'] + '/services/' + config['yarn_service_name'] + '/yarnApplications?executing=false&from=' + config['from'] + '&to=' + config['to'] + '&limit=' + config['page_size'] + '&offset=' + str(offset) + '&filter=application_type=' + application_type + ' and pool=' + config['pool'], auth=(config['user_name'], passwd), verify='ca.pem')

        response_count = len(response.json()['applications'])
        offset += int(config['page_size'])

        app_list = [app for app in response.json()['applications']]

        for app in app_list:
            if 'allocatedMemorySeconds' in app and 'allocatedVcoreSeconds' in app and 'startTime' in app and 'endTime' in app:
                analyzed_applications.append([application_type, app['applicationId'], app['user'], app['allocatedMemorySeconds'], app['allocatedVcoreSeconds']])
                analyzed_applications_count += 1

                startTime = app['startTime']
                endTime = app['endTime']
                duration = (datetime.strptime(endTime, "%Y-%m-%dT%H:%M:%S.%fZ") - datetime.strptime(startTime, "%Y-%m-%dT%H:%M:%S.%fZ")).total_seconds() * 1000
                mem = int(math.ceil(app['allocatedMemorySeconds'] / duration))
                cores = int(math.ceil(app['allocatedVcoreSeconds'] / duration))

                start_resources = {
                    'timestamp': startTime,
                    'count': 1,
                    'mem': mem,
                    'cores': cores
                }

                end_resources = {
                    'timestamp': endTime,
                    'count': -1,
                    'mem': -mem,
                    'cores': -cores
                }

                resource_events.append(start_resources)
                resource_events.append(end_resources)
            else:
                skipped_applications.append([app['applicationId']])
                skipped_applications_count += 1

        if response_count < int(config['page_size']):
            if len(response.json()['warnings']) == 1:
                config['to'] = response.json()['warnings'][0].split()[-1]
                offset = 0
            else:
                break

    # Function to sort resource events by timestamp
    def bytimestamp(resource_event):
        return resource_event['timestamp']

    # After sorting the resource_events by timestamp
    current_mem = 0
    current_cores = 0
    peak_mem = 0
    peak_cores = 0
    peak_mem_start_time = None
    peak_mem_end_time = None
    peak_cores_start_time = None
    peak_cores_end_time = None

for event in resource_events:
    current_mem += event['mem']
    current_cores += event['cores']

    # Check for new peak in memory utilization
    if current_mem > peak_mem:
        peak_mem = current_mem
        peak_mem_start_time = event['timestamp']  # Update start time for new peak memory period
        peak_mem_end_time = None  # Reset end time

    # Check for new peak in vcore utilization
    if current_cores > peak_cores:
        peak_cores = current_cores
        peak_cores_start_time = event['timestamp']  # Update start time for new peak cores period
        peak_cores_end_time = None  # Reset end time

    # Update the end time for the memory peak period
    if peak_mem_end_time is None and event['count'] < 0 and current_mem < peak_mem:
        peak_mem_end_time = event['timestamp']

    # Update the end time for the cores peak period
    if peak_cores_end_time is None and event['count'] < 0 and current_cores < peak_cores:
        peak_cores_end_time = event['timestamp']

# Report the peak utilization details
print(f'Peak Memory Utilization: {peak_mem} MB')
print(f'Peak Memory Utilization Window: {peak_mem_start_time} to {peak_mem_end_time}')
print(f'Peak VCore Utilization: {peak_cores} cores')
print(f'Peak VCore Utilization Window: {peak_cores_start_time} to {peak_cores_end_time}')
