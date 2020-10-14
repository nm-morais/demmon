FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git

#demmon
WORKDIR $GOPATH/src/github.com/nm-morais/deMMon
COPY . .

#deps
COPY --from=nmmorais/go-babel:latest /src/github.com/nm-morais/go-babel ../go-babel/
COPY --from=nmmorais/demmon-exporter:latest /src/github.com/nm-morais/deMMon-exporter ../deMMon-exporter/
RUN go get -d -v ./...

#build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/deMMon cmd/demmon/main.go

# EXECUTABLE IMG
FROM alpine:edge as demmon

VOLUME [ "/var/lib/influxdb" ]

LABEL APP="influxdb"
LABEL APP_REPOSITORY="https://dl.influxdata.com/influxdb/releases/"

COPY scripts/setupTc.sh /setupTc.sh
COPY config/influxdb.conf /etc/influxdb/influxdb.conf
COPY build/influxdb-entrypoint.sh /influxdb-entrypoint.sh
COPY build/docker-entrypoint.sh /docker-entrypoint.sh
COPY --from=builder /go/bin/deMMon /go/bin/deMMon
ARG LATENCY_MAP=config/inet100Latencies_x0.005.txt
ARG IPS_MAP=config/ips100.txt
COPY ${LATENCY_MAP} /latencyMap.txt
COPY ${IPS_MAP} /ips.txt
RUN chmod +x /setupTc.sh /docker-entrypoint.sh /go/bin/deMMon /influxdb-entrypoint.sh

ENV TIMEZONE Europe/Paris
ENV INFLUXDB_VERSION 1.8.3

# Installing packages
ADD https://dl.influxdata.com/influxdb/releases/influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz ./
RUN apk add --no-cache su-exec
RUN apk add iproute2-tc
RUN addgroup influxdb && adduser -s /bin/false -G influxdb -S -D influxdb
RUN apk --no-cache add curl
RUN tar -C . -xzf influxdb-${INFLUXDB_VERSION}-static_linux_amd64.tar.gz && \
        chmod +x influxdb-*/* && \
        cp -a influxdb-*/* /usr/bin/ && \
        rm -rf *.tar.gz* influxdb-*/


ENTRYPOINT ["/docker-entrypoint.sh", "/latencyMap.txt", "/ips.txt"]