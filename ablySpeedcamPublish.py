#!/usr/bin/env python
"""
This script will read each line from the speed_cam.db from the DB
Publish the data to Ably
Write a new field to the DB to denote that it has been sent

Installation
------------

    sudo apt-get update
    sudo apt-get install sqlite3


"""
from __future__ import print_function
import sys
import os
import time
from ably import AblyRest
my_path = os.path.abspath(__file__)  # Find the full path of this python script
# get the path location only (excluding script name)
base_dir = my_path[0:my_path.rfind("/")+1]
base_file_name = my_path[my_path.rfind("/")+1:my_path.rfind(".")]
prog_name = os.path.basename(__file__)

# Check for variable file to import and error out if not found.
config_file_path = os.path.join(base_dir, "config.py")
if not os.path.exists(config_file_path):
    print("ERROR : Missing config.py file - File Not Found %s"
          % config_file_path)
    sys.exit(1)
# Read Configuration variables from config.py file
from config import *

try:
    import sqlite3
except ImportError:
    print("ERROR: Problem importing sqlite3. Try Installing per")
    print("       sudo apt-get install sqlite3")
    sys.exit(1)

PROG_VER = "ver 0.1"

#=================
# User Variables
#=================
VERBOSE_ON = True
WAIT_SECS = 30  # seconds to wait between queries for images to process

#=================
# System Variables
#=================
# Find the full path of this python script
MY_PATH = os.path.abspath(__file__)
# get the path location only (excluding script name)
BASE_DIR = MY_PATH[0:MY_PATH.rfind("/")+1]
# BASE_FILE_NAME is This script name without extension
BASE_FILE_NAME = MY_PATH[MY_PATH.rfind("/")+1:MY_PATH.rfind(".")]
PROG_NAME = os.path.basename(__file__)
ably = AblyRest(ably_api_key)
channel = ably.channels.get(ably_channel)
def Main():

    HORZ_LINE = "----------------------------------------------------------------------"
    if VERBOSE_ON:
        print(HORZ_LINE)
        print("%s %s   written by Richie Jarvis" % (PROG_NAME, PROG_VER))
        print("Ably publisher for speed_cam.py Images")
        print("Based upon Claude Pageau's speed_cam available here: https://github.com/pageauc/speed-camera")
        print(HORZ_LINE)
        print("Loading   Wait ...")

    try:
        RETRY = 0
        while True:
            try:
                # Connect to sqlite3 database file
                DB_CONN = sqlite3.connect(db_file)
            except sqlite3.Error as err_msg:
                print("ERROR: Failed sqlite3 Connect to DB %s"
                      % db_file)
                print("       %s" % err_msg)
                RETRY += 1
                if RETRY <= 5:
                    print('DB_CONN RETRY %i/5  Wait ...' % RETRY)
                    time.sleep(5)
                    continue  # loop
                else:
                    sys.exit(1)

            # setup CURSOR for processing db query rows
            CURSOR = DB_CONN.cursor()
            CURSOR.execute("SELECT * FROM speed WHERE status=''")
            ALL_ROWS = CURSOR.fetchall()
            DB_CONN.close() # Close DB to minimize db lock issues
            ROW_TOTAL = len(ALL_ROWS)  # Get total Rows to Process
            ROW_MSG = "%i Rows Found to Process." % ROW_TOTAL
            ROW_COUNTER = 0
            PLATES_DONE = 0
            for row in ALL_ROWS:
                if row[9] == "L2R":
                    direction = "Southbound"
                else:
                    direction = "Northbound"
                ROW_INDEX =  row[0]
                timestamp =  make_date(ROW_INDEX)
                speed = row[3]
                speed_unit = row[4]
                record = {
                    '@timestamp' : timestamp,
                    'actual_time': ROW_INDEX,
                    'speed' : speed,
                    'direction' : direction,
                    'source' : source_camera,
                    'speed_unit' : speed_unit
                    }

                ROW_COUNTER += 1
                # Now publish this record to Ably via REST
                # channel.publish("event",record)
                try:
                    channel.publish("event",record)
                    ABLY_PUBLISHED = True
                except:
                    ABLY_PUBLISHED = False
                    print("Something Bad Happened")


                # UPDATE speed_cam.db speed, status column with ABLY_PUBLISHED or NULL
                try:
                    DB_CONN = sqlite3.connect(db_file)
                    if ABLY_PUBLISHED:
                        if VERBOSE_ON:
                            print("%i/%i Published to Ably" %
                                  (ROW_COUNTER, ROW_TOTAL
                                ))
                        SQL_CMD = ('''UPDATE speed SET status="ABLY_PUBLISHED" WHERE idx="{}"'''
                               .format(ROW_INDEX))
                    else:
                        if VERBOSE_ON:
                            print("%i/%i Not published" %
                                  (ROW_COUNTER, ROW_TOTAL))
                        # set speed table status field to NULL
                        SQL_CMD = ('''UPDATE speed SET status=NULL WHERE idx="{}"'''
                               .format(ROW_INDEX))
                    DB_CONN.execute(SQL_CMD)
                    DB_CONN.commit()
                    DB_CONN.close()
                except sqlite3.OperationalError:
                    print("SQLITE3 DB Lock Problem Encountered.")
                ROW_MSG = "Published %i Speeds" % PLATES_DONE
            if VERBOSE_ON:
                print('%s  Wait %is ...' % (ROW_MSG, WAIT_SECS))
            time.sleep(WAIT_SECS)

    except KeyboardInterrupt:
        print("")
        print("%s %s User Exited with ctr-c" %(PROG_NAME, PROG_VER))
    finally:
        print("DB_CONN.close %s" % DB_FILE)
        DB_CONN.close()
        print("%s %s  Bye ..." % (PROG_NAME, PROG_VER))

def make_date(string):
    # 0123456789012345
    # YYYYMMDD-hhmmsss
    YYYY = string[0:4]
    MM = string[4:6]
    DD = string[6:8]
    hh = string[9:11]
    mm = string[11:13]
    string = (YYYY + '-' + MM + '-' + DD + 'T' + hh + ':'+ mm + ':00' + timezone).strip()
    return string


if __name__ == "__main__":
    Main()
