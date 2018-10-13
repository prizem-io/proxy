# STEP 1 build executable binary

FROM golang:1.11.1-alpine as builder

# Install SSL ca certificates
RUN apk update && apk add git && apk add ca-certificates

# Create appuser
RUN adduser -D -g '' appuser

RUN mkdir /app && chown appuser /app
COPY . $GOPATH/src/github.com/prizem-io/proxy
WORKDIR $GOPATH/src/github.com/prizem-io/proxy
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -ldflags="-w -s" -o /build/proxy cmd/proxy/main.go


# STEP 2 build a small image

FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /build/proxy /app/proxy
COPY --from=builder /app /app
WORKDIR /app
USER appuser

ENTRYPOINT ["/app/proxy"]
