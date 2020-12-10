alias run_rsync='rsync -razP --delete /Users/nunomorais/go/src/github.com/nm-morais/ dicluster:/home/nunomorais/git/nm-morais'
run_rsync; fswatch -or . | while read f; do run_rsync; done

# rsync -rv /Users/nunomorais/go/src/github.com/nm-morais/ dicluster:/home/nunomorais/git/nm-morais