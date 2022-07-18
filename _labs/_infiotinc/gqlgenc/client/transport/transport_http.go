package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"reflect"
	"strings"
)

type HttpRequestOption func(req *http.Request)

type Http struct {
	URL string
	// Client defaults to http.DefaultClient
	Client           *http.Client
	RequestOptions   []HttpRequestOption
	UseFormMultipart bool
}

func (h *Http) Request(req Request) Response {
	opres, err := h.request(req)
	if err != nil {
		return NewErrorResponse(err)
	}

	return NewSingleResponse(*opres)
}

func (h *Http) request(gqlreq Request) (*OperationResponse, error) {
	if h.Client == nil {
		h.Client = http.DefaultClient
	}

	bodyb, err := json.Marshal(NewOperationRequestFromRequest(gqlreq))
	if err != nil {
		return nil, err
	}

	var req *http.Request
	if h.UseFormMultipart {
		req, err = h.formReq(gqlreq, bodyb)
		if err != nil {
			return nil, err
		}
	} else {
		req, err = http.NewRequestWithContext(gqlreq.Context, "POST", h.URL, bytes.NewReader(bodyb))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
	}

	for _, ro := range h.RequestOptions {
		ro(req)
	}

	res, err := h.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var opres OperationResponse
	err = json.Unmarshal(data, &opres)
	if err != nil {
		return nil, err
	}

	if len(opres.Data) == 0 && len(opres.Errors) == 0 {
		return nil, fmt.Errorf("no data nor errors, got %v: %.1000s", res.StatusCode, data)
	}

	return &opres, nil
}

func (h *Http) jsonFormField(w *multipart.Writer, name string, v interface{}) error {
	fw, err := w.CreateFormField(name)
	if err != nil {
		return err
	}

	return json.NewEncoder(fw).Encode(v)
}

func (h *Http) formReq(gqlreq Request, bodyb []byte) (*http.Request, error) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)

	filesMap := make(map[string][]string)

	i := 0
	for p, f := range h.collectUploads("variables", gqlreq.Variables) {
		k := fmt.Sprintf("%v", i)
		fw, err := w.CreateFormFile(k, f.Name)
		if err != nil {
			return nil, err
		}

		// Write file to field
		if _, err := io.Copy(fw, f.File); err != nil {
			return nil, err
		}

		filesMap[k] = []string{p}
		i++
	}

	err := w.WriteField("operations", string(bodyb))
	if err != nil {
		return nil, err
	}

	err = h.jsonFormField(w, "map", filesMap)
	if err != nil {
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", h.URL, &b)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	return req, nil
}

func (h *Http) collectUploads(path string, in interface{}) map[string]Upload {
	if up, ok := in.(Upload); ok {
		return map[string]Upload{
			path: up,
		}
	}

	v := reflect.ValueOf(in)
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		rs := make(map[string]Upload)
		for i := 0; i < v.Len(); i++ {
			p := fmt.Sprintf("%v.%v", path, i)
			for fk, f := range h.collectUploads(p, v.Index(i).Interface()) {
				rs[fk] = f
			}
		}
		return rs
	case reflect.Struct:
		rs := make(map[string]Upload)
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)

			if !f.CanInterface() {
				continue // private field
			}

			ft := v.Type()
			k := ft.Field(i).Tag.Get("json")

			if strings.Contains(k, ",") {
				i := strings.Index(k, ",")
				k = k[:i]
			}

			if k == "-" {
				continue
			}

			p := fmt.Sprintf("%v.%v", path, k)
			for fk, f := range h.collectUploads(p, f.Interface()) {
				rs[fk] = f
			}
		}
		return rs
	case reflect.Map:
		rs := make(map[string]Upload)
		iter := v.MapRange()
		for iter.Next() {
			p := fmt.Sprintf("%v.%v", path, iter.Key().Interface())
			for fk, f := range h.collectUploads(p, iter.Value().Interface()) {
				rs[fk] = f
			}
		}
		return rs

	case reflect.Ptr:
		if v.IsNil() {
			return nil
		}

		return h.collectUploads(path, v.Elem().Interface())
	}

	return nil
}
