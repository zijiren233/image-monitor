FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder

WORKDIR /monitor

COPY ./ /monitor

ARG GOARCH

RUN if [ -z "$GOARCH" ]; then echo "GOARCH is not set"; exit 1; fi

RUN GOARCH=$GOARCH go build -trimpath -tags "jsoniter" -ldflags "-s -w" -o monitor

FROM alpine:latest

RUN mkdir -p /monitor

WORKDIR /monitor

RUN apk add --no-cache ca-certificates tzdata && \
    rm -rf /var/cache/apk/*

COPY --from=builder /monitor/monitor /usr/local/bin/monitor

ENTRYPOINT ["monitor"]
