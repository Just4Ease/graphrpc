# GraphRPC

----

## About

GraphRPC is simply GraphQL as your RPC Contract Input & Output Layer and NATS.io as your data transmission via events. (
Request/Reply, Pub&Sub )

- No proto contract corruption on any update
- Programming language agnostic
- One entry point
- Custom headers on query || mutations
- Client code generation ( thanks to https://github.com/Yamashou/gqlgenc 🚀 )
- Nats.io integration
- Server CodeGen ( using https://github.com/99designs/gqlgen )

## Appreciation & Inspirations

- 99designs - https://github.com/99designs/gqlgen
- Yamashou - https://github.com/Yamashou/gqlgenc
- NATS.io - https://nats.io
- GraphQL - https://graphql.org
- gRPC - https://grpc.io
- Axon - https://github.com/Just4Ease/axon
- AxonRPC - https://github.com/Just4Ease/axonrpc

## TODO

- [] Subscriptions for clients (WIP)
- TLS on server startup.

## How to use

```shell script
# To generate server code
# It is advised to add this command to a makefile or run the tools.go once 
printf '// +build tools\npackage tools\nimport _ "github.com/Just4Ease/graphrpc/generator/cmd"' | gofmt > tools.go
go mod tidy

# To actually generate resolvers and server entrypoint file.
go run github.com/Just4Ease/graphrpc/generator/cmd --filename server.go
```