
if [ "$#" -ne 1 ]; then
    echo "usage: syncFilesGrid.sh <site>"
    exit 1
fi

run_rsync='rsync --progress -razPv --exclude 'demmon_logs' --exclude 'thesis_evaluation/stats' --exclude 'thesis_evaluation/config/ips_file.txt' --exclude 'histogram.png' /Users/nunomorais/go/src/github.com/nm-morais/ $1.g5k:/home/nmorais/git/nm-morais'
eval $run_rsync; fswatch -or . | while read f; do eval $run_rsync; done

# rsync -rv /Users/nunomorais/go/src/github.com/nm-morais/ dicluster:/home/nunomorais/git/nm-morais
