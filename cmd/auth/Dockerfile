ARG NODE_OS_NAME
ARG NODE_OS_TAG
ARG BUILDER

FROM ${BUILDER} as builder
RUN go test -timeout 10s ./cmd/auth -v
RUN go install -v ./cmd/auth

FROM ${NODE_OS_NAME}:${NODE_OS_TAG}
WORKDIR /opt/trekt/
COPY --from=builder /go/bin/auth .
ENTRYPOINT ["./auth"]