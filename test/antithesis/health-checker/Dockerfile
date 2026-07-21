# syntax=docker/dockerfile:1

FROM golang:1.26.5-bookworm AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/health-checker ./main.go

FROM scratch

COPY --from=build /out/health-checker /health-checker

ENTRYPOINT ["/health-checker"]
