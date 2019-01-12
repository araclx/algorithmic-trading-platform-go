ARG NODE_OS_NAME
ARG NODE_OS_TAG
ARG BUILDER

FROM ${BUILDER} as builder
RUN go install -v ./cmd/trekt-auth

FROM ${NODE_OS_NAME}:${NODE_OS_TAG}
WORKDIR /opt/trekt/
COPY --from=builder /go/bin/trekt-auth .
ENTRYPOINT ["./trekt-auth"]