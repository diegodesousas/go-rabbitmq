build-publisher-example:
	go build -o bin/publisher examples/publisher/main.go

test:
	./gotestsum --format=pkgname --junitfile tests.xml -- -race -covermode=atomic -coverprofile cover.out ./...

watch:
	./gotestsum --format=pkgname --watch -- -race ./...