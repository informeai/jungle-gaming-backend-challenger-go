# syntax=docker/dockerfile:1
# Go version must match go.mod (go 1.26.5).
FROM golang:1.26.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/wallet-service ./cmd/wallet-service

FROM alpine:3.22
RUN adduser -D -u 10001 app && apk add --no-cache ca-certificates wget
COPY --from=build /out/wallet-service /usr/local/bin/wallet-service
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/wallet-service"]
CMD ["serve"]
