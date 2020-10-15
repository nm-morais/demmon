FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git
RUN apk add --no-cache make gcc gawk bison linux-headers libc-dev
#demmon
WORKDIR $GOPATH/src/github.com/nm-morais/deMMon
COPY . .

#deps
COPY --from=nmmorais/go-babel:latest /src/github.com/nm-morais/go-babel ../go-babel/
# COPY --from=nmmorais/demmon-exporter:latest /src/github.com/nm-morais/deMMon-exporter ../deMMon-exporter/
RUN go get -d -v ./...

#build
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -race -ldflags="-w -s" -o /go/bin/deMMon cmd/demmon/main.go

# EXECUTABLE IMG
FROM alpine:edge as demmon

RUN apk add iproute2-tc

COPY scripts/setupTc.sh /setupTc.sh
COPY build/docker-entrypoint.sh /docker-entrypoint.sh
COPY --from=builder /go/bin/deMMon /go/bin/deMMon
ARG LATENCY_MAP=config/inet100Latencies_x0.005.txt
ARG IPS_MAP=config/ips100.txt
COPY ${LATENCY_MAP} /latencyMap.txt
COPY ${IPS_MAP} /ips.txt
RUN chmod +x /setupTc.sh /docker-entrypoint.sh /go/bin/deMMon

ENTRYPOINT ["/docker-entrypoint.sh", "/latencyMap.txt", "/ips.txt"]