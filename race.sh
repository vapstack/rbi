while true; do
  go test -race -count=5 -timeout=60m ./... || break
done
