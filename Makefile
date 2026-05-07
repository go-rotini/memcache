.PHONY: all clean lint test test-acceptance test-bench test-corpus test-fuzz test-mutation test-race

all: clean lint test test-bench test-fuzz test-mutation test-race

clean:
	@rm -rf *.out test_mutation.json test_bench.out

lint:
	@test -z "$$(gofmt -l .)" || (echo "files not formatted:" && gofmt -l . && exit 1)
	go vet ./...
	go mod verify
	go tool golangci-lint run ./...
	go tool go-licenses check ./...
	go tool govulncheck ./...

test:
	@go test -v -count=1 -coverprofile=test.out ./...
	@go tool cover -func=test.out

test-acceptance:
	@go test -v -count=1 -run TestAcceptance -coverprofile=test_acceptance.out ./...
	@go tool cover -func=test_acceptance.out

test-bench:
	@go test -bench=. -benchmem -count=5 -run='^$$' ./... | tee test_bench.out

test-corpus:
	@go test -v -count=1 -run TestCorpus -coverprofile=test_corpus.out ./...
	@go tool cover -func=test_corpus.out

test-fuzz:
	@go test -fuzz=FuzzCacheOps -fuzztime=60s -run=^$$ .
	@go test -fuzz=FuzzSnapshot -fuzztime=60s -run=^$$ .
	@go test -fuzz=FuzzLoader -fuzztime=60s -run=^$$ .
	@go test -fuzz=FuzzKeyHash -fuzztime=30s -run=^$$ .

test-mutation:
	@go tool github.com/go-gremlins/gremlins/cmd/gremlins unleash --config .gremlins.yaml

test-race:
	@go test -race -count=1 -coverprofile=test_race.out ./...
	@go tool cover -func=test_race.out
