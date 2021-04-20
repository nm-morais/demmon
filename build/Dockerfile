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
RUN GOOS=linux GOARCH=amd64 go build -o /go/bin/demmon cmd/demmon/*.go

# EXECUTABLE IMG
FROM debian:stable-slim as demmon

# RUN apk add iproute2-tc
RUN apt update 2>/dev/null | grep -P "\d\K upgraded" ; apt install iproute2 -y 2>/dev/null ; apt install kmod -y 2>/dev/null

COPY build/docker-entrypoint.sh /docker-entrypoint.sh
COPY --from=builder /go/bin/demmon /go/bin/demmon

# ARG LATENCY_MAP=config/inet100Latencies_x0.04.txt
# ARG IPS_FILE=config/generated_config.txt

ARG LATENCY_MAP
ARG IPS_FILE

COPY ${LATENCY_MAP} /latencyMap.txt
COPY ${IPS_FILE} /config.txt

RUN chmod +x /docker-entrypoint.sh /go/bin/demmon
RUN mkdir /tmp/logs

ENTRYPOINT ["/docker-entrypoint.sh", "/latencyMap.txt"]
