# MCP Edge Relay

Low-latency cloud edge for an OpenAI MCP tunnel backed by a local Desktop Commander instance.

The cloud side terminates MCP lifecycle traffic and forwards only real tool work to the PC over an authenticated outbound link. Runtime tunnel credentials are delivered at bootstrap and kept in memory only.

## Runtime

- Node.js relay (`server.mjs`)
- OpenAI `tunnel-client` pinned and built in the Dockerfile
- No secrets committed to the repository

## Required environment

- `FIXED_TUNNEL_ID`

The PC-side edge agent supplies the runtime tunnel key during bootstrap.
