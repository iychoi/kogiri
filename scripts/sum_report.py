#! /usr/bin/env python

import os
import os.path
import sys
import re
from datetime import timedelta

JOB_STRING = "Job : "
JOB_STATUS = "Status : "
JOB_START_TIME = "StartTime : "
JOB_FINISH_TIME = "FinishTime : "
JOB_TIME_TAKEN = "TimeTaken : "
JOB_HDFS_READ = "HDFS: Number of bytes read="
JOB_HDFS_WRITTEN = "HDFS: Number of bytes written="
JOB_INTERMEDIATE_DATA_GENERATED = "Reduce shuffle bytes="

JOB_STATUS_SUCCEEDED = "SUCCEEDED"

extract_pattern = [
    JOB_STRING,
    JOB_STATUS,
    JOB_START_TIME,
    JOB_FINISH_TIME,
    JOB_TIME_TAKEN,
    JOB_HDFS_READ,
    JOB_HDFS_WRITTEN,
    JOB_INTERMEDIATE_DATA_GENERATED
]

regex_duration = re.compile(r'((?P<hours>\d+?)h\s*)?((?P<minutes>\d+?)m\s*)?((?P<seconds>\d+?)s)?')

def parse_time(time_str):
    parts = regex_duration.match(time_str)
    if not parts:
        return
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.iteritems():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params)

def extract_data(line, pattern):
    idx = line.find(pattern)
    return line[idx + len(pattern):].strip()

def data_size(size):
    sizeB = size
    sizeKB = sizeB/1024.0
    sizeMB = sizeKB/1024.0
    sizeGB = sizeMB/1024.0
    sizeTB = sizeGB/1024.0
    return str(sizeB) + " B\n\t" + str(sizeKB) + " KB\n\t" + str(sizeMB) + " MB\n\t" + str(sizeGB) + " GB\n\t" + str(sizeTB) + " TB"

# parse report file
def parse(filename):
    f = open(filename, 'r')

    # prepare jobs
    jobs = []
    dataline = []
    for line in f:
        # handle line
        if JOB_STRING in line:
            idx = line.find(JOB_STRING)
            dataline.append(line[:idx])
            jobs.append(dataline)
            dataline = []
            dataline.append(line[idx:].strip())
        else:
            dataline.append(line.strip())

    jobs.append(dataline)
    dataline = []
    f.close()

    # extract useful
    totalTimeTaken = timedelta(0)
    totalBytesRead = 0
    totalBytesWritten = 0
    totalIntermediateBytes = 0
    jobFailedTitle = []
    jobSucceededTitle = []
    for dataline in jobs:
        jobTitle = ""
        for line in dataline:
            found = False
            for pattern in extract_pattern:
                if pattern in line:
                    found = True
                    if pattern == JOB_TIME_TAKEN:
                        timeTakenStr = extract_data(line, JOB_TIME_TAKEN)
                        timeTaken = parse_time(timeTakenStr)
                        totalTimeTaken += timeTaken

                    if pattern == JOB_HDFS_READ:
                        bytesStr = extract_data(line, JOB_HDFS_READ)
                        totalBytesRead += long(bytesStr)

                    if pattern == JOB_HDFS_WRITTEN:
                        bytesStr = extract_data(line, JOB_HDFS_WRITTEN)
                        totalBytesWritten += long(bytesStr)                        

                    if pattern == JOB_INTERMEDIATE_DATA_GENERATED:
                        bytesStr = extract_data(line, JOB_INTERMEDIATE_DATA_GENERATED)
                        totalIntermediateBytes += long(bytesStr)                        

                    if pattern == JOB_STRING:
                        jobTitle = extract_data(line, JOB_STRING)

                    if pattern == JOB_STATUS:
                        statusStr = extract_data(line, JOB_STATUS)
                        if statusStr.strip() != JOB_STATUS_SUCCEEDED:
                            jobFailedTitle.append(jobTitle)
                        else:
                            jobSucceededTitle.append(jobTitle)

                    break
            if found:
                print line

    print "=========================="
    print "Summary"
    print "Job Succeeded :", len(jobSucceededTitle)
    print "Job Failed :", len(jobFailedTitle)
    print "Total Time Taken :", totalTimeTaken
    print "Total Bytes Read :", data_size(totalBytesRead)
    print "Total Bytes Written :", data_size(totalBytesWritten)
    print "Total Intermediate Data in Bytes:", data_size(totalIntermediateBytes)

def main(argv):
    if len(argv) < 1:
        print "command : ./sum_report.py report_file"
    else:
        report = argv[0]
        tasks = parse(report)

if __name__ == "__main__":
    main(sys.argv[1:])
