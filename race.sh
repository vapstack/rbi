while true; do
  go test -race -timeout=30m ./... || break
done
