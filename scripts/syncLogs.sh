set -e 

if [ $# -eq 0 ]
  then
    echo "No node name supplied"
    exit 1
fi

echo "copying logs from node $1"

# ssh dicluster 'scp -r node7:/tmp/demmon_logs/ ~/'; scp -r dicluster:demmon_logs /tmp/; code /tmp/demmon_logs
copyLogsFromNodeCmd="rsync --delete -razP $1:/tmp/demmon_logs/ ~/demmon_logs/"

echo "running $copyLogsFromNodeCmd"

ssh dicluster $copyLogsFromNodeCmd; rsync --delete -razP dicluster:demmon_logs/ /tmp/demmon_logs/; code /tmp/demmon_logs