ARG GO_VER
ARG NODE_OS_NAME
ARG NODE_OS_TAG

###############################################################################

FROM golang:${GO_VER}-${NODE_OS_NAME}${NODE_OS_TAG} as builder

WORKDIR /go/src/github.com/rektra-network/trekt-go

COPY . .

RUN apk add --no-cache git
RUN go get -d -v ./cmd/trekt-auth 
RUN go install -v ./cmd/trekt-auth 

###############################################################################

FROM ${NODE_OS_NAME}:${NODE_OS_TAG}

WORKDIR /opt/trekt/

COPY --from=builder /go/bin/trekt-auth .

ENTRYPOINT ["./trekt-auth"]

###############################################################################