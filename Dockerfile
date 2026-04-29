# syntax=docker/dockerfile:1

# Build the manager binary
ARG GO_VERSION=1.25
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

# Copy the Go Modules manifests
COPY --link go.mod go.sum ./
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Copy the go source
COPY --link . .

# Build
# GOARCH defaults to the requested target architecture from buildx.
# For local `make docker-build`, this matches your host arch; for multi-arch `make docker-push`,
# buildx sets per-platform values while keeping the Dockerfile unchanged.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-w -s" -o /manager ./cmd

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static-debian13:nonroot AS final
WORKDIR /
COPY --link --from=builder /manager /manager

ENTRYPOINT ["/manager"]
