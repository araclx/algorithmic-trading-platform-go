ARG TAG

FROM golang:${TAG}

WORKDIR /go/src/github.com/rektra-network/trekt-go
COPY . .

RUN apk update
RUN apk add git
RUN apk add build-base

RUN go get -v golang.org/x/crypto/acme/autocert
RUN go get -v github.com/gorilla/mux
RUN go get -v github.com/gorilla/websocket
RUN go get -v github.com/mitchellh/mapstructure
RUN go get -v github.com/streadway/amqp
RUN go get -v github.com/Shopify/sarama

RUN go get -v github.com/golang/mock/gomock
RUN go get -v github.com/golang/mock/mockgen
RUN make mock
RUN go test -timeout 15s \
  ./pkg/trekt \
  ./pkg/tradinglib \
  -v