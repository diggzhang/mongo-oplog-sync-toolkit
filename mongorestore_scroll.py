#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    MongoDB Point In Time Sync Tool Kit
    Mongorestore scroll tool
"""
import os
import sys
import subprocess
from pymongo import MongoClient
import logging
from utils import mongo_uri


logging.basicConfig(filename='oplogSyncFlow.log', level=logging.INFO)
local_mongo_client = MongoClient(host="127.0.0.1", port=27017)
local_op_status_coll = local_mongo_client["sync"]["currentOplog"]


def oplog_exists(dump_folder):
    file_location = "./dump/{}/local/oplog.rs.bson".format(dump_folder)
    return os.path.exists(file_location)


def mongorestore(op_item):
    op_timestamp = op_item['opEnd']
    dump_folder = op_timestamp['t'] + "_" + op_timestamp['i']
    cp_bson = "cp ./dump/{}/local/oplog.rs.bson ./dump/oplog.bson".format(dump_folder)
    restore_cmd = "mongorestore --host {} --oplogReplay --dir ./dump".format(mongo_uri.LOCAL_MONGO_URI)
    remove_bson = "rm ./dump/oplog.bson"
    file_name = "restore_with_range" + dump_folder + ".sh"
    with open(file_name, "w") as f:
        logging.info(restore_cmd)
        f.write(cp_bson + "\n")
        f.write(restore_cmd + "\n")
        f.write(remove_bson + "\n")
    if oplog_exists(dump_folder):
        subprocess.check_output(['sh', file_name])
    else:
        logging.info("[*] Oplog dump file is not exists.")
        sys.exit(-1)


def restore_queue():
    restore_item = local_op_status_coll.find_one({"isDump": True, "isRestore": False})
    logging.info("==--" * 15)
    logging.info("   Restore sync time " + str(restore_item["syncTime"]))
    logging.info("==--" * 15)
    mongorestore(restore_item)
    local_op_status_coll.update_one({"_id": restore_item["_id"]}, {"$set": {"isRestore": True}})


if __name__ == "__main__":
    queue_length = local_op_status_coll.count({"isRestore": False, "isDump": True})
    if queue_length is not 0:
        queue_length = local_op_status_coll.count({"isDump": True, "isRestore": False})
        logging.info("[*] Length of restore queue is " + str(queue_length))
        for item in range(0, queue_length):
            restore_queue()
    else:
        logging.info("[*] Restore queue is empty.")
        sys.exit(-1)

    logging.info("[*] Oplog restore done")
