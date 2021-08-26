# GraphRPC

----

## About
GraphRPC is simply GraphQL as your RPC Contract Input & Output Layer.

- No proto contract corruption on any update
- Programming language agnostic
- One entry point
- Client code generation ( thanks to https://github.com/Yamashou/gqlgenc 🚀 )
- Nats.io integration
- todo: Server CodeGen ( using https://github.com/99designs/gqlgen ) 



## Appreciation & Inspirations

- 99designs - https://github.com/99designs/gqlgen
- Yamashou - https://github.com/Yamashou/gqlgenc
- NATS.io - https://nats.io
- GraphQL - https://graphql.org
- gRPC - https://grpc.io
- Axon - https://github.com/Just4Ease/axon
- AxonRPC - https://github.com/Just4Ease/axonrpc


## TODO

- [] Subscriptions
- [] Custom Headers on query
- [] Use cloud events semantics || axonrpc/messages (msgpack) for marshaling & unmarshaling 
- [] Add Server generator following 99designs/gqlgen's implementation
- [] Discover method
