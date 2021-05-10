



run_rsync='rsync --progress -razP --exclude 'demmon_logs' --exclude 'thesis_evaluation/stats' --exclude 'thesis_evaluation/config/ips_file.txt' --exclude 'histogram.png' /Users/nunomorais/go/src/github.com/nm-morais/ nancy.g5k:/home/nmorais/git/nm-morais'
eval $run_rsync; fswatch -or . | while read f; do eval $run_rsync; done

# rsync -rv /Users/nunomorais/go/src/github.com/nm-morais/ dicluster:/home/nunomorais/git/nm-morais
