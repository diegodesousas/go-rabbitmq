build-publisher-example:
	go build -o bin/publisher examples/publisher/main.go

test:
	go test -race -covermode=atomic -coverprofile cover.out ./...