#!/bin/bash
set -e # exit on error
export PATH=~/.pyenv/shims:~/.pyenv/bin:"$PATH"

#---------------------------------------------------------------------
# What: 增量oplog拉取
#
# comment: 传入线上阿里云mongodb的db和表名字，
# 抽取该db下该表的oplog回滚到线下某个库下
#
# Built-in tools:     脚本内用到的unix工具或其他工具（最好写明绝对路径）
# Internal script:    该脚本内包含的其他脚本，最好写明相对路径
# Pre-execution:      该脚本的前置脚本
# Post-execution:     该脚本的后置脚本
# Output:             该脚本的output是什么
#
# author:       xingze
# contact:	    xingze@guanghe.tv/ diggzhang@gmail.com
# since:        Tue Apr 23 11:37:56 CST 2019
#
# Update: date - description 脚本修改时间 以及相关描述
#
#---------------------------------------------------------------------

#---------------------------------------------------------------------
# SCRIPT CONFIGURATION
#---------------------------------------------------------------------

SCRIPT_NAME=$(basename "$0")
VERSION=0.1

# Global variables
# Todo: uncomment this
# YEAR=$(date -d -0day '+%Y')
# MONTH=$(date -d -0day '+%m')
# DAY=$(date -d -0day y '+%Y')
YEAR=$(date '+%Y')
MONTH=$(date '+%m')
DAY=$(date '+%d')
LOGFILE=/tmp/oplog_sync_"$1"_"$2"_"$YEAR$MONTH$DAY".log
# make sure we're in the directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

#---------------------------------------------------------------------
# UTILITY FUNCTIONS
#---------------------------------------------------------------------

# log message to screen and log file
log ()
{
    echo "[${SCRIPT_NAME}]: $1" >> "$LOGFILE"
    echo "[${SCRIPT_NAME}]: $1"
}

# Define script functions here

run_a() {
  python mongooplog_scroll.py $1 $2
}

run_b() {
  date
  python mongodump_scroll.py $1 $2
}

run_c() {
  date
  python mongorestore_scroll.py
}

main() {
  DB="$1"
  TABLE="$2"
  echo $DB $TABLE
  log "Script running in $SCRIPT_DIR"
  log "version $VERSION"
  log "(1/3) mongooplog_scroll"
  run_a $DB $TABLE >> "$LOGFILE"
  log "(2/3) mongodump_scroll"
  run_b >> "$LOGFILE"
  log "(3/3) mongorestore_scroll"
  run_c >> "$LOGFILE"
}

main "$@"
