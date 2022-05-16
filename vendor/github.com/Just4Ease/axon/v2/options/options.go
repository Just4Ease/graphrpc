package options

type Options struct {
	// Used to select codec
	ServiceName         string
	Address             string
	CertContent         string
	AuthenticationToken string
	Username            string
	Password            string

	// Other options for implementations of the interface
}

//var (
//	DefaultContentType = "application/msgpack"
//
//	DefaultCodecs = map[string]codec.NewCodec{
//		"application/json":         json.NewCodec,
//		"application/msgpack":      msgpack.NewCodec,
//		"application/protobuf":     proto.NewCodec,
//		"application/octet-stream": raw.NewCodec,
//	}
//)
