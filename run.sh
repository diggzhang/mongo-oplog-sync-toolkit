# mongo in time backup sync cronjob
# 1 */1 * * * /bin/bash /pathto/Scripts/mongo-oplog-sync/run.sh >> /tmp/mongo_sync.log 2>&1
export PATH=~/.pyenv/shims:~/.pyenv/bin:"$PATH"
cd /pathto/Scripts/mongo-oplog-sync
echo ">>> Oplog "`date`
python mongooplog_scroll.py >> /tmp/mongo_sync.log
echo ">>> Dump"`date`
python mongodump_scroll.py >> /tmp/mongo_sync.log
echo ">>> Restore"`date`
python mongorestore_scroll.py >> /tmp/mongo_sync.log
echo ">>> Done"`date`
