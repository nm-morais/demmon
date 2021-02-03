FROM golang:latest AS builder

# RUN apk update && apk add --no-cache git
# RUN apk add --no-cache make gcc gawk bison linux-headers libc-dev
#demmon
WORKDIR $GOPATH/src/github.com/nm-morais/demmon
COPY . .

#deps
COPY --from=nmmorais/go-babel:latest /src/github.com/nm-morais/go-babel ../go-babel/
COPY --from=nmmorais/demmon-common:latest /src/github.com/nm-morais/demmon-common ../demmon-common/
COPY --from=nmmorais/demmon-client:latest /src/github.com/nm-morais/demmon-client ../demmon-client/
COPY --from=nmmorais/demmon-exporter:latest /src/github.com/nm-morais/demmon-exporter ../demmon-exporter/
# RUN go get -d -v ./...
RUN go mod download

#build
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build --race -ldflags="-w -s" -o /go/bin/demmon cmd/demmon/*.go

# EXECUTABLE IMG
FROM debian:stable-slim as demmon

# RUN apk add iproute2-tc
RUN apt update 2>/dev/null | grep -P "\d\K upgraded" ; apt install iproute2 -y 2>/dev/null

COPY scripts/setupTc.sh /setupTc.sh
COPY build/docker-entrypoint.sh /docker-entrypoint.sh
COPY --from=builder /go/bin/demmon /go/bin/demmon
ARG LATENCY_MAP=config/inet100Latencies_x0.04.txt
ARG IPS_MAP=config/ips100.txt
COPY ${LATENCY_MAP} /latencyMap.txt
COPY ${IPS_MAP} /ips.txt
RUN chmod +x /docker-entrypoint.sh /go/bin/demmon

ENTRYPOINT ["/docker-entrypoint.sh", "/latencyMap.txt", "/ips.txt"]