---
title = "MCP Server"
description = "Optional MCP transport for Nexum server, exposing query, object, and maintenance operations as tools."
layout = "reference"
eyebrow = "Integration"
lead = "Nexum MCP support lives entirely in nexum-server as a transport and orchestration layer over the existing services."
statusLabel = "Server Boundary"
accent = "Protocol only"
---

# MCP Server

Status: optional server transport

The MCP surface is implemented entirely in `nexum-server`.

It does not change or extend the core database engine in `lib`. The MCP layer is only:

- transport
- protocol adaptation
- tool registration
- tool dispatch to existing server services

## Endpoint

- `POST /mcp`

The current implementation supports MCP-style JSON-RPC calls for:

- `initialize`
- `tools/list`
- `tools/call`

## Current Tools

- `nexum.query`
- `nexum.object.register_type`
- `nexum.object.put`
- `nexum.object.get`
- `nexum.object.find`
- `nexum.object.delete`
- `nexum.maintenance.status`
- `nexum.maintenance.checkpoint`

## Example

Initialize:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2026-04-04"
  }
}
```

Call a tool:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "tools/call",
  "params": {
    "name": "nexum.query",
    "arguments": {
      "provider": "sql",
      "payload": "SELECT * FROM users"
    }
  }
}
```

## Architecture

The MCP layer sits alongside the existing REST API.

Call chain:

- MCP request
- `McpController`
- `McpToolRegistry`
- MCP tool handler
- existing Nexum server service
- core `lib`

That keeps the boundary clean:

- `lib` remains unaware of MCP
- `server` owns the protocol surface
- business logic remains in the existing service layer
