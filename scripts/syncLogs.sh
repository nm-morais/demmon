set -e 

if [ $# -eq 0 ]
  then
    echo "No node name supplied"
    exit 1
fi

echo "copying logs from nodes $@"
ssh dicluster 'rm -rf ~/demmon_logs/*'

for node in "$@"
do
  copyLogsFromNodeCmd="rsync -raz $node:/tmp/demmon_logs/ ~/demmon_logs/"
  echo "running $copyLogsFromNodeCmd"
  ssh dicluster $copyLogsFromNodeCmd &
done

wait

rsync --delete -raz dicluster:demmon_logs/ /tmp/demmon_logs/; code /tmp/demmon_logs