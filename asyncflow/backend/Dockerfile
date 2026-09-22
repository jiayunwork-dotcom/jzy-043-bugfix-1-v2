# Build a static binary; no cgo is needed (pure-Go pgx + redis clients).
FROM golang:1.22-alpine AS build
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/asyncflow ./cmd/server

# ---- runtime ----
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget
WORKDIR /app
COPY --from=build /out/asyncflow /app/asyncflow
COPY migrations /app/migrations
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=10 \
  CMD wget -qO- http://localhost:8080/api/health || exit 1
ENTRYPOINT ["/app/asyncflow", "-migrations", "/app/migrations"]
