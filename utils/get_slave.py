#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess
import logging


def get_salve_uri(mongoMasterUri, mongoSlaveUri, mongoPassword):
    mongo_test_is_slave_script = "isSlave.js"
    cmd_check_ismaster = "mongo --quiet --host " + mongoMasterUri + ":3717 --authenticationDatabase admin -u root -p " + mongoPassword + " ./isSlave.js"
    isMaster = subprocess.check_output(cmd_check_ismaster.split(' ')).decode("utf-8").replace('\n', '')

    if isMaster == "true":
        logging.info("Probe " + mongoMasterUri + " is master.")
        targetUri = mongoSlaveUri
    else:
        logging.info("Probe " + mongoMasterUri + " is slave.")
        targetUri = mongoMasterUri

    logging.info("Target slave uri: " + targetUri + ".")
    return targetUri
