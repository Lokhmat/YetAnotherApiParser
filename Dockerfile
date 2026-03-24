FROM golang:1.24.1 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/api-parser ./cmd/main.go

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=build /out/api-parser /usr/local/bin/api-parser
COPY bundle /app/bundle

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/api-parser", "-config", "/app/bundle/config.yaml"]
