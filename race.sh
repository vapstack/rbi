while true; do
  go test -race -count=3 -timeout=0 ./... || break
done
