FROM golang:1.10.3-alpine3.7
RUN apk update && apk add git
RUN mkdir -p /go/src/github.com/sledigabel
ADD . /go/src/github.com/sledigabel/sir
RUN go get -u github.com/golang/dep/...
WORKDIR /go/src/github.com/sledigabel/sir/
RUN dep ensure
RUN go test -v -parallel 1 ./...
WORKDIR /go/src/github.com/sledigabel/sir/cmd
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o sir .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=0 /go/src/github.com/sledigabel/sir/cmd/sir .
ENTRYPOINT ["./sir"]