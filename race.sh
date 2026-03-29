while true; do
  go test -race -count=3 -timeout=30m ./... || break
done
