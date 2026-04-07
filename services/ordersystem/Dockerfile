FROM golang:1.23 AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ordersystem ./cmd

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=builder /app/ordersystem /app/ordersystem

EXPOSE 8081
ENTRYPOINT ["/app/ordersystem"]
