FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o sidecar ./cmd

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata \
    && addgroup -S app \
    && adduser -S -G app app \
    && mkdir -p /data/sidecar/files \
    && chown -R app:app /data/sidecar

WORKDIR /app

COPY --from=builder /app/sidecar /app/sidecar
COPY --from=builder /app/files /app/files

ENV APP_TIMEZONE=Asia/Kolkata \
    SIDECAR_HTTP_ADDR=:8082 \
    SIDECAR_WEIGHTS_PATH=/app/files/official_nifty50_weights.csv \
    SIDECAR_OUTPUT_PATH=/data/sidecar/files/dragger_puller.json

EXPOSE 8082

USER app

ENTRYPOINT ["/app/sidecar"]
