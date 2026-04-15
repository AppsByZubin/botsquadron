FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o marketfeeder ./cmd

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata \
    && addgroup -S app \
    && adduser -S -G app app

WORKDIR /app

COPY --from=builder /app/marketfeeder /app/marketfeeder

USER app

ENTRYPOINT ["/app/marketfeeder"]
