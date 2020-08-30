set -e


if [ -z $SWARM_VOL_DIR ]; then
  echo "env var SWARM_VOL_DIR not defined"
  exit
fi


rm -rf logs/ ; mkdir logs

host=$(hostname)
for node in $(oarprint host); do
  if [ $node != $host ]; then
    scp -r $node:$SWARM_VOL_DIR/* logs &
  fi
done
cp -r $SWARM_VOL_DIR/* logs/
wait

tar -cvf logs.tar logs/