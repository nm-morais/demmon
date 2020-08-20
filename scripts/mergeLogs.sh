set -e

rm -rf logs/ ; mkdir logs

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    scp -r $node:$SWARM_VOL_DIR/* logs
  fi
done

cp -r $SWARM_VOL_DIR/* logs/