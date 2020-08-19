ARG LATENCY_MAP=config/inet100Latencies_x0.005.txt
ARG IPS_MAP=config/ips100.txt
ARG LISTEN_PORT=1200

FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git

WORKDIR $GOPATH/src/github.com/nm-morais/deMMon
COPY . .
RUN cd ../; git clone https://github.com/nm-morais/go-babel.git # manually download latest 
RUN go get -d -v ./...
RUN GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/deMMon cmd/membership/main.go

FROM alpine:latest

COPY scripts/setupTc.sh /setupTc.sh
COPY build/docker-entrypoint.sh /docker-entrypoint.sh
COPY ${LATENCY_MAP} /config/latencyMap.txt
COPY ${IPS_MAP} /config/ips.txt
COPY --from=builder /go/bin/deMMon /go/bin/deMMon

RUN chmod +x /setupTc.sh
RUN chmod +x /docker-entrypoint.sh
RUN chmod +x /go/bin/deMMon

EXPOSE ${LISTEN_PORT}/tcp
EXPOSE ${LISTEN_PORT}/udp

ENTRYPOINT ["/docker-entrypoint.sh"]