run_rsync='rsync -razP --delete --exclude 'demmon_logs' --exclude 'histogram.png' /Users/nunomorais/go/src/github.com/nm-morais/ dicluster:/home/nunomorais/git/nm-morais'
eval $run_rsync; fswatch -or . | while read f; do eval $run_rsync; done

# rsync -rv /Users/nunomorais/go/src/github.com/nm-morais/ dicluster:/home/nunomorais/git/nm-morais