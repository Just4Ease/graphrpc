package transport

import (
	"fmt"
	"io"
	"os"
)

func NewUpload(f *os.File) Upload {
	return Upload{
		File: f,
		Name: f.Name(),
	}
}

type Upload struct {
	Name string
	File io.Reader
}

func (u Upload) MarshalJSON() ([]byte, error) {
	return []byte(`null`), nil // Should be marshaled to null, will be taken care of in multipart form
}

func (u *Upload) UnmarshalJSON(data []byte) error {
	return fmt.Errorf("type Upload should not be unmarshaled")
}
