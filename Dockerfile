# Multi-stage build for the raft-kv node binary.
#
# Build:
#   docker build -t raft-kv .
#
# The resulting image's entrypoint is the same binary and flags documented
# in the README's "Running a real cluster" section; docker-compose.yml
# wires up a 3-node cluster using it.

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/node ./cmd/node

FROM alpine:3.20
COPY --from=build /out/node /usr/local/bin/node
ENTRYPOINT ["node"]
