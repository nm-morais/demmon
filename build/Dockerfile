FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git

WORKDIR $GOPATH/src/github.com/nm-morais/deMMon
COPY . .
RUN cd ../; git clone https://github.com/nm-morais/go-babel.git # manually download latest 
RUN go get -d -v ./...
RUN GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/deMMon cmd/membership/main.go

FROM scratch
COPY --from=builder /go/bin/deMMon /go/bin/deMMon
ENTRYPOINT ["/go/bin/deMMon"]