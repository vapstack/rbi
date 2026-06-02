while true; do
  go test -race -count=5 -timeout=0 ./... || break
done
