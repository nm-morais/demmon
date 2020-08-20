FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git

WORKDIR $GOPATH/src/github.com/nm-morais/deMMon
COPY . .
RUN cd ../; git clone https://github.com/nm-morais/go-babel.git # manually download latest 
RUN go get -d -v ./...
RUN GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/deMMon cmd/membership/main.go

FROM alpine:latest

RUN apk add iproute2

COPY scripts/setupTc.sh /setupTc.sh
COPY build/docker-entrypoint.sh /docker-entrypoint.sh
COPY --from=builder /go/bin/deMMon /go/bin/deMMon

ARG LATENCY_MAP=config/inet100Latencies_x0.005.txt
ARG IPS_MAP=config/ips100.txt

COPY ${LATENCY_MAP} /latencyMap.txt
COPY ${IPS_MAP} /ips.txt

RUN chmod +x /setupTc.sh /docker-entrypoint.sh /go/bin/deMMon

ENTRYPOINT ["/docker-entrypoint.sh", "/latencyMap.txt", "/ips.txt"]