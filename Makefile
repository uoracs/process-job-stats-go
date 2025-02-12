.PHONY: build install clean run container handler
all: build

build:
	@go build -o bin/process-job-stats-go cmd/process-job-stats-go/main.go

run: build
	@go run cmd/process-job-stats-go/main.go

install: build
	@cp bin/process-job-stats-go /usr/local/bin/process-job-stats-go

container:
	@docker build -t process-job-stats-go .

handler:
	@go build -o handler cmd/process-job-stats-go/main.go

clean:
	@rm -f bin/process-job-stats-go /usr/local/bin/process-job-stats-go
