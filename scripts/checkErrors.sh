
errors="false"

while getopts ":he" opt; do
  case ${opt} in
    h ) 
        echo "Available flags:"
        echo "-e        show errors"
        exit 0
      ;;
    e )
        errors="true"
      ;;
    \? ) echo "Usage: cmd [-h] [-e]"
        exit 1
      ;;
  esac
done


containers=$(docker ps | awk '{if(NR>1) print $NF}')

for container in $containers
do
    echo " "
    echo "==================$container=================="
    logs=$`(docker logs $container)`

    echo " "
    echo "=========DATA RACES========="
    echo " "
    echo "$logs" | grep "WARNING: DATA RACE"

    echo " "
    echo "=========PANICS========="
    echo " "
    echo "$logs" | grep "panic"

    if [ "$errors" = "true" ]; then
        echo " "
        echo "=========ERRORS========="
        echo "$logs" | grep "error"
        echo " "
    fi
done