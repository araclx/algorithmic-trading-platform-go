ARG NODE_OS_NAME
ARG NODE_OS_TAG
ARG BUILDER

FROM ${BUILDER} as builder
RUN go test -timeout 10s ./cmd/exchange/binance -v
RUN go install -v ./cmd/exchange/binance

FROM ${NODE_OS_NAME}:${NODE_OS_TAG}
WORKDIR /opt/trekt/
COPY --from=builder /go/bin/binance .
RUN apk add --no-cache ca-certificates
ENTRYPOINT ["./binance"]