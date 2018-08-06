cat mongodump_scroll.py
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    MongoDB Point In Time Sync Tool Kit
    Mongodump scroll tool
"""
import sys
import subprocess
from pymongo import MongoClient
from string import Template
import logging
from utils import mongo_uri


logging.basicConfig(filename='oplogSyncFlow.log', level=logging.INFO)
local_mongo_client = MongoClient(host="127.0.0.1", port=27017)
local_op_status_coll = local_mongo_client["sync"]["currentOplog"]


def mongodump(op_item):
    start, end = op_item['opStart'], op_item['opEnd']
    target_uri = op_item['syncFrom']
    dump_folder = end['t'] + "_" + end['i']

    cmd_dump = "mongodump --host " + target_uri + ":3717 --authenticationDatabase admin -u root -p " + mongo_uri.PASSWD + " --db local --collection oplog.rs --query "
    cmd_query = Template('{"ns": "onions.users", "ts": {"$gte": {"$timestamp": {"t": $st,"i": $si}}, "$lt": {"$timestamp": {"t": $et,"i": $ei}}}}').substitute(gte="$gte", lt="$lt", timestamp="$timestamp", st=start['t'], si=start['i'], et=end['t'], ei=end['i'])
    cmd = cmd_dump + "\'" + cmd_query + "\'" + " -o ./dump/" + dump_folder
    file_name = "dump_with_range" + dump_folder + ".sh"
    with open(file_name, "w") as f:
        logging.info(cmd)
        f.write(cmd)
    try:
        subprocess.check_output(['sh', file_name])
    except Exception as e:
        print("FirstTimeDumpFailed " + str(e))
        subprocess.check_output(['sh', file_name])    
    


def dump_queue():
    dump_item = local_op_status_coll.find_one({"isDump": False})
    logging.info("==--" * 15)
    logging.info("    Dump sync time " + str(dump_item["syncTime"]))
    logging.info("==--" * 15)
    mongodump(dump_item)
    local_op_status_coll.update_one({"_id": dump_item["_id"]}, {"$set": {"isDump": True}})


if __name__ == "__main__":
    queue_length = local_op_status_coll.count({"isDump": False})
    if queue_length == 0:
        logging.info("[*] Dump queue is empty.")
        sys.exit(-1)

    logging.info("[*] Length of queue is " + str(queue_length))

    for item in range(0, queue_length):
        dump_queue()
    logging.info("[*] Oplog dump done")
