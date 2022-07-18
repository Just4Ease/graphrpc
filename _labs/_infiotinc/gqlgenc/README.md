# gqlgenc

> gqlgenc is a fully featured go gql client, powered by codegen 

## Why yet another Go GQL client ?

| Package                                     | Codegen | Subscription | Extensions |
|---------------------------------------------|---------|--------------|------------|
| https://github.com/shurcooL/graphql         | ❌      | ❌            |❌          |
| https://github.com/Yamashou/gqlgenc         | ✅      | ❌            |❌          |
| https://github.com/hasura/go-graphql-client | ❌      | ✅            |❌          |
| ✨[https://github.com/infiotinc/gqlgenc](https://github.com/infiotinc/gqlgenc)✨| ✅ | ✅ | ✅ |

## GQL Client

### Transports

gqlgenc is transport agnostic, and ships with 3 transport implementations:

- http: Transports GQL queries over http
- ws: Transports GQL queries over websocket
- split: Can be used to have a single client use multiple transports depending on the type of query (`query`, `mutation` over http and `subscription` over ws)

### Quickstart

Quickstart with a client with http & ws transports:

```go
package main

import (
    "context"
    "github.com/infiotinc/gqlgenc/client"
    "github.com/infiotinc/gqlgenc/client/transport"
)

func main() {
    wstr := &transport.Ws{
        URL: "wss://example.org/graphql",
    }
    wstr.Start(context.Background())
    defer wstr.Close()

    httptr := &transport.Http{
        URL: "https://example.org/graphql",
    }

    tr := transport.SplitSubscription(wstr, httptr)

    cli := &client.Client {
        Transport: tr,
    }
}
```

### Query/Mutation

```go
var res struct {
    Room string `json:"room"`
}
_, err := cli.Query(ctx, "", "query { room }", nil, &res) // or Mutation
if err != nil {
    panic(err)
}
```

### Subscription

```go
sub, stop := cli.Subscription(ctx, "", "subscription { newRoom }", nil)
defer stop()

for sub.Next() {
    msg := sub.Get()
    
    if len(msg.Errors) > 0 {
        // Do something with them
    }
    
    var res struct {
        Room string `json:"newRoom"`
    }
    err := msg.UnmarshalData(&res)
    if err != nil {
        // Do something with that
    }
}

if err := sub.Err(); err != nil {
    panic(err)
}
```

## GQL Client Codegen

Create a `.gqlgenc.yml` at the root of your module:

```yaml
client:
  package: graph
  filename: ./graph/gen_client.go
models:
  Int:
    model: github.com/99designs/gqlgen/graphql.Int64
  DateTime:
    model: github.com/99designs/gqlgen/graphql.Time
# The schema can be fetched from files or through introspection
schema:
  - schema.graphqls
endpoint:
  url: https://api.annict.com/graphql # Where do you want to send your request?
  headers:　# If you need header for getting introspection query, set it
    Authorization: "Bearer ${ANNICT_KEY}" # support environment variables
query:
  - query.graphql

```

Fill your `query.graphql` with queries:
```graphql
query GetRoom {
    room(name: "secret room") {
        name
    }
}
```

Run `go run github.com/infiotinc/gqlgenc`

Enjoy:
```go
// Create codegen client
gql := &graph.Client{
    Client: cli,
}

gql.GetRoom(...)
```

## Input as `map`

In Go, working with JSON and nullity can be tricky. The recommended way to deal with such case is through maps. You can ask gqlgenc to generate such maps with helpers through config:

Globally:
```yaml
client:
  input_as_map: true
```

Per model:
```yaml
models:
  SomeInput:
    as_map: true
```

## Extensions

### APQ

[Automatic Persisted Queries](https://www.apollographql.com/docs/apollo-server/performance/apq/) can be enabled by adding:

```go
cli.Use(&extensions.APQ{})
```

## File Upload

- In the `Http` transport, set `UseFormMultipart` to `true`

- In `.gqlgenc.yaml`:

```yaml
models:
  Upload:
    model: github.com/infiotinc/gqlgenc/client/transport.Upload
```

- Profit

```go
up := transport.NewUpload(someFile)

_, _, err := gql.MyUploadFile(ctx, up)
```

## Acknowledgements

This repo is based on the great work of [Yamashou/gqlgenc](https://github.com/Yamashou/gqlgenc) and [hasura/go-graphql-client](https://github.com/hasura/go-graphql-client)
