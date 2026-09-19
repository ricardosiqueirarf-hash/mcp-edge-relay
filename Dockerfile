FROM golang:1.27-bookworm AS tunnel-builder
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
ARG TUNNEL_CLIENT_SHA=9f24ddcf265d60a2e506a4ab9369376cd234440e
RUN git clone https://github.com/openai/tunnel-client.git /src/tunnel-client \
 && cd /src/tunnel-client \
 && git checkout "$TUNNEL_CLIENT_SHA" \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tunnel-client ./cmd/client

FROM node:24-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=tunnel-builder /out/tunnel-client /usr/local/bin/tunnel-client
WORKDIR /app
COPY server.mjs /app/server.mjs
ENV NODE_ENV=production
CMD ["node", "/app/server.mjs"]
