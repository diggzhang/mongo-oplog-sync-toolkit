#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    MongoDB Point In Time Sync Tool Kit
    Mongooplog scroll split tool
"""
import subprocess
from pymongo import MongoClient, DESCENDING
from string import Template
import re
from datetime import datetime
from utils import get_slave, mongo_uri
import logging


logging.basicConfig(filename='oplogSyncFlow.log', level=logging.INFO)
local_mongo_client = MongoClient(host="127.0.0.1", port=27017)
local_op_status_coll = local_mongo_client["sync"]["currentOplog"]


opsync_status = {
    'opStart': {'t': 0, 'i': 0},
    'opEnd': {'t': 0, 'i': 0},
    'isRestore': False,
    'isDump': False,
    'syncTime': datetime.now(),
}


def fetch_newest_oplog_timestamp(target_uri):
    cmd_fetch_newest_oplog_timestamp = Template('''
#!/usr/bin/env bash
mongo --quiet --host $mongo_uri:3717 --authenticationDatabase admin -u root -p $password local --eval 'db.oplog.rs.find({"ns": "onions.users"}, {"ts":1}).sort({"$natural": -1}).limit(1)'
''').substitute(mongo_uri=target_uri, password=mongo_uri.PASSWD)
    file_name = "newestTimestamp.sh"
    with open(file_name, "w") as f:
        f.write(cmd_fetch_newest_oplog_timestamp)
    latest_timestamp = subprocess.check_output(['bash', file_name]).decode("utf-8").replace('\n', '')
    timestamps = re.findall(r"\d+", latest_timestamp)
    return {'t': timestamps[0], 'i': timestamps[1]}


def fetch_pre_oplog_timestamp(opsync_status):
    if local_op_status_coll.count() == 0:
        opsync_setter(opsync_status)
        return {'t': 0, 'i': 0}
    pre_op = list(local_op_status_coll.find().sort('syncTime', DESCENDING))[0]
    return pre_op['opEnd']


def opsync_setter(opsync_status):
    local_op_status_coll.insert(opsync_status)


if __name__ == "__main__":
    logging.info("[*] Fetch slave uri.")
    target_uri = get_slave.get_salve_uri(mongo_uri.MONGO_MASTER_URI, mongo_uri.MONGO_SLAVE_URI, mongo_uri.PASSWD)
    opsync_status['syncFrom'] = target_uri
    opsync_status['syncTime'] = datetime.now()
    logging.info("[*] Fetch newest oplog timestamp.")
    newest_oplog_timestamp = fetch_newest_oplog_timestamp(target_uri)
    logging.info(newest_oplog_timestamp)
    opsync_status['opEnd'] = newest_oplog_timestamp
    logging.info("[*] Fetch previous sync oplog timestamp.")
    pre_oplog_timestamp = fetch_pre_oplog_timestamp(opsync_status)
    opsync_status['opStart'] = pre_oplog_timestamp
    logging.info("[*] Update opsync status collection")
    opsync_setter(opsync_status)
    logging.info("[*] Oplog scroll done")
