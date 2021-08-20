// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/ocsp"
)

func TestOCSPAlwaysMustStapleAndShutdown(t *testing.T) {
	// Certs that have must staple will auto shutdown the server.
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tcOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}

	tlsConf, err := server.GenTLSConfig(tcOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeAlways,
		OverrideURLs: []string{addr},
	}
	srv := RunServer(&opts)
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := getOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					return fmt.Errorf("invalid staple")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// The server will shutdown because the server becomes revoked
	// and the policy is to always must-staple.  The OCSP Responder
	// instructs the NATS Server to fetch OCSP Staples every 2 seconds.
	time.Sleep(2 * time.Second)
	setOCSPStatus(t, addr, serverCert, ocsp.Revoked)
	time.Sleep(2 * time.Second)

	// Should be connection refused since server will abort now.
	_, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nats.ErrNoServers {
		t.Errorf("Expected connection refused")
	}
}

func TestOCSPMustStapleShutdown(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-status-request-cert.pem"
		serverKey  = "configs/certs/ocsp/server-status-request-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tlsConfigOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}

	tlsConf, err := server.GenTLSConfig(tlsConfigOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeMust,
		OverrideURLs: []string{addr},
	}

	srv := RunServer(&opts)
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := getOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					return fmt.Errorf("invalid staple")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// The server will shutdown because the server becomes revoked
	// and the policy is to always must-staple.  The OCSP Responder
	// instructs the NATS Server to fetch OCSP Staples every 2 seconds.
	time.Sleep(2 * time.Second)
	setOCSPStatus(t, addr, serverCert, ocsp.Revoked)
	time.Sleep(2 * time.Second)

	// Should be connection refused since server will abort now.
	_, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nats.ErrNoServers {
		t.Errorf("Expected connection refused")
	}
}

func TestOCSPMustStapleAutoDoesNotShutdown(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)

	content := `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	defer removeFile(t, conf)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := getOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// The server will shutdown because the server becomes revoked
	// and the policy is to always must-staple.  The OCSP Responder
	// instructs the NATS Server to fetch OCSP Staples every 2 seconds.
	time.Sleep(2 * time.Second)
	setOCSPStatus(t, addr, serverCert, ocsp.Revoked)
	time.Sleep(2 * time.Second)

	// Should not be connection refused, the client will continue running and
	// be served the stale OCSP staple instead.
	_, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := getOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Revoked {
					t.Errorf("Expected revoked status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestOCSPAutoWithoutMustStapleDoesNotShutdownOnRevoke(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)

	opts := server.Options{}
	opts.Host = "127.0.0.1"
	opts.NoLog = true
	opts.NoSigs = true
	opts.MaxControlLine = 4096
	opts.Port = -1
	opts.TLSCert = serverCert
	opts.TLSKey = serverKey
	opts.TLSCaCert = caCert
	opts.TLSTimeout = 5
	tlsConfigOpts := &server.TLSConfigOpts{
		CertFile: opts.TLSCert,
		KeyFile:  opts.TLSKey,
		CaFile:   opts.TLSCaCert,
		Timeout:  opts.TLSTimeout,
	}
	tlsConf, err := server.GenTLSConfig(tlsConfigOpts)
	if err != nil {
		t.Fatal(err)
	}
	opts.TLSConfig = tlsConf

	opts.OCSPConfig = &server.OCSPConfig{
		Mode:         server.OCSPModeAuto,
		OverrideURLs: []string{addr},
	}

	srv := RunServer(&opts)
	defer srv.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("Unexpected OCSP staple for certificate")
				}
				return nil
			},
		}),

		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Revoke the client certificate, nothing will happens since does
	// not have MustStaple.
	time.Sleep(2 * time.Second)
	setOCSPStatus(t, addr, serverCert, ocsp.Revoked)
	time.Sleep(2 * time.Second)

	// Should not be connection refused since server will continue running.
	_, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
}

func TestOCSPClient(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	ocspURL := fmt.Sprintf("http://%s", ocspr.Addr)
	defer ocspr.Shutdown(ctx)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"OCSP Stapling makes server fail to boot if status is unknown",
			`
				port: -1

				# Enable OCSP stapling with policy to honor must staple if present.
				ocsp: true

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {},
		},
		{
			"OCSP Stapling ignored by default if server without must staple status",
			`
				port: -1

				ocsp: true

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() { setOCSPStatus(t, ocspURL, serverCert, ocsp.Good) },
		},
		{
			"OCSP Stapling honored by default if server has must staple status",
			`
				port: -1

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				setOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
			},
		},
		{
			"OCSP Stapling can be disabled even if server has must staple status",
			`
				port: -1

				ocsp: false

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				setOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Revoked)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			defer removeFile(t, conf)
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()

			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()

			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()

			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}
		})
	}
}

func TestOCSPReloadRotateTLSCertWithNoURL(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)

	content := `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	defer removeFile(t, conf)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := getOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Change the contents with another that will fail to get a staple
	// since it does not have an URL.
	content = `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	if err := ioutil.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	// Reload show warning because of cert missing OCSP Url so cannot be used
	// with OCSP stapling.
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}
	expectedErr := fmt.Errorf("missing OCSP response")
	// The server will not shutdown because the reload will fail.
	_, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				// The new certificate does not have OCSP Staples since
				// it could not fetch one from a OCSP server.
				if s.OCSPResponse == nil {
					return expectedErr
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != expectedErr {
		t.Fatalf("Unexpected error: %s", expectedErr)
	}
}

func TestOCSPReloadRotateTLSCertDisableMustStaple(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)

	storeDir := createDir(t, "_ocsp")
	defer removeDir(t, storeDir)

	originalContent := `
		port: -1

		store_dir: "%s"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`

	content := fmt.Sprintf(originalContent, storeDir)
	conf := createConfFile(t, []byte(content))
	defer removeFile(t, conf)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	var staple []byte
	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				staple = s.OCSPResponse
				resp, err := getOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	files := []string{}
	err = filepath.Walk(storeDir+"/ocsp/", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			t.Error(err)
		}
		if bytes.Equal(staple, data) {
			found = true
		}
	}
	if !found {
		t.Error("Could not find OCSP Staple")
	}

	// Change the contents with another that has OCSP Stapling disabled.
	updatedContent := `
		port: -1

		store_dir: "%s"

		tls {
			cert_file: "configs/certs/ocsp/server-cert.pem"
			key_file: "configs/certs/ocsp/server-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	content = fmt.Sprintf(updatedContent, storeDir)
	if err := ioutil.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}

	// The new certificate does not have must staple so they will be missing.
	time.Sleep(4 * time.Second)

	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("unexpected OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Re-enable OCSP Stapling
	content = fmt.Sprintf(originalContent, storeDir)
	if err := ioutil.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}

	var newStaple []byte
	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				newStaple = s.OCSPResponse
				resp, err := getOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Confirm that it got a new staple.
	files = []string{}
	err = filepath.Walk(storeDir+"/ocsp/", func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, file := range files {
		data, err := ioutil.ReadFile(file)
		if err != nil {
			t.Error(err)
		}
		if bytes.Equal(newStaple, data) {
			found = true
		}
	}
	if !found {
		t.Error("Could not find OCSP Staple")
	}
	if bytes.Equal(staple, newStaple) {
		t.Error("Expected new OCSP Staple")
	}
}

func TestOCSPReloadRotateTLSCertEnableMustStaple(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)
	setOCSPStatus(t, addr, updatedServerCert, ocsp.Good)

	// Start without OCSP Stapling MustStaple
	content := `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-cert.pem"
			key_file: "configs/certs/ocsp/server-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	defer removeFile(t, conf)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("unexpected OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Change the contents with another that has OCSP Stapling enabled.
	content = `
		port: -1

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	if err := ioutil.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}

	// The new certificate does not have must staple so they will be missing.
	time.Sleep(2 * time.Second)

	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				resp, err := getOCSPStatus(s)
				if err != nil {
					return err
				}
				if resp.Status != ocsp.Good {
					t.Errorf("Expected valid OCSP staple status")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func TestOCSPCluster(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := createDir(t, "_ocspA")
	defer removeDir(t, storeDirA)
	storeDirB := createDir(t, "_ocspB")
	defer removeDir(t, storeDirB)
	storeDirC := createDir(t, "_ocspC")
	defer removeDir(t, storeDirC)

	// Seed server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		cluster {
			name: AB
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	defer removeFile(t, sconfA)
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// The rest
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		cluster {
			name: AB
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1

			routes: [ nats://127.0.0.1:%d ]
			connect_retries: 30

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.Cluster.Port)
	conf := createConfFile(t, []byte(srvConfB))
	defer removeFile(t, conf)
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	checkClusterFormed(t, srvA, srvB)

	// Revoke the seed server cluster certificate, following servers will not be able to verify connection.
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Revoked)

	// Original set of servers still can communicate to each other, even though the cert has been revoked.
	// NOTE: Should we unplug from the cluster in case our server is revoke and OCSP policy is always or must?
	checkClusterFormed(t, srvA, srvB)

	// Wait for seed server to notice that its certificate has been revoked,
	// so that new routes can't connect to it.
	time.Sleep(6 * time.Second)

	// Start another server against the seed server that has an invalid OCSP Staple
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-05-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-05-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		cluster {
			name: AB
			host: "127.0.0.1"
			advertise: 127.0.0.1
			port: -1

			routes: [ nats://127.0.0.1:%d ]
			connect_retries: 30

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.Cluster.Port)
	conf = createConfFile(t, []byte(srvConfC))
	defer removeFile(t, conf)
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	cB, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	cC, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}

	// There should be no connectivity between the clients due to the revoked staple.
	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()
	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()
	resp, err := cC.Request("foo", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}
	resp, err = cC.Request("bar", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}

	// Switch the certs from the seed server to new ones that are not revoked,
	// this should restart OCSP Stapling for the cluster routes.
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-07-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-07-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		cluster {
			port: -1
			name: AB
			host: "127.0.0.1"
			advertise: 127.0.0.1
			connect_retries: 30

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := ioutil.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	// Wait to get a new OCSP Staple.
	time.Sleep(10 * time.Second)
	checkClusterFormed(t, srvA, srvB, srvC)

	// Now clients connect to C can communicate with B and A.
	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOCSPLeaf(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := createDir(t, "_ocspA")
	defer removeDir(t, storeDirA)
	storeDirB := createDir(t, "_ocspB")
	defer removeDir(t, storeDirB)
	storeDirC := createDir(t, "_ocspC")
	defer removeDir(t, storeDirC)

	// LeafNode server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	defer removeFile(t, sconfA)
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// LeafNode that has the original as a remote.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		leafnodes {
			remotes: [ {
				url: "tls://127.0.0.1:%d"
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			} ]
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.LeafNode.Port)
	conf := createConfFile(t, []byte(srvConfB))
	defer removeFile(t, conf)
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	// checkLeafNodeConnected(t, srvA)

	// Revoke the seed server cluster certificate, following servers will not be able to verify connection.
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Revoked)

	// Original set of servers still can communicate to each other, even though the cert has been revoked.
	// checkLeafNodeConnected(t, srvA)

	// Wait for seed server to notice that its certificate has been revoked,
	// so that new leafnodes can't connect to it.
	time.Sleep(6 * time.Second)

	// Start another server against the seed server that has an invalid OCSP Staple
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-05-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-05-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		leafnodes {
			remotes: [ {
				url: "tls://127.0.0.1:%d"
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			} ]
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.LeafNode.Port)
	conf = createConfFile(t, []byte(srvConfC))
	defer removeFile(t, conf)
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	cB, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	cC, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}

	// There should be no connectivity between the clients due to the revoked staple.
	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()
	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()
	resp, err := cC.Request("foo", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}
	resp, err = cC.Request("bar", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}

	// Switch the certs from the leafnode server to new ones that are not revoked,
	// this should restart OCSP Stapling for the leafnode server.
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-07-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-07-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		leafnodes {
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := ioutil.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)

	// A <-> A
	_, err = cA.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}

	// B <-> A
	_, err = cB.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}

	// C <-> A
	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
	// C <-> B via leafnode A
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOCSPGateway(t *testing.T) {
	const (
		caCert = "configs/certs/ocsp/ca-cert.pem"
		caKey  = "configs/certs/ocsp/ca-key.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-03-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-04-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-05-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-06-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-07-cert.pem", ocsp.Good)
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-08-cert.pem", ocsp.Good)

	// Store Dirs
	storeDirA := createDir(t, "_ocspA")
	defer removeDir(t, storeDirA)
	storeDirB := createDir(t, "_ocspB")
	defer removeDir(t, storeDirB)
	storeDirC := createDir(t, "_ocspC")
	defer removeDir(t, storeDirC)

	// Gateway server configuration
	srvConfA := `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-02-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-02-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	sconfA := createConfFile(t, []byte(srvConfA))
	defer removeFile(t, sconfA)
	srvA, optsA := RunServerWithConfig(sconfA)
	defer srvA.Shutdown()

	// LeafNode that has the original as a remote.
	srvConfB := `
		host: "127.0.0.1"
		port: -1

		server_name: "BBB"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-03-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-03-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		gateway {
			name: B
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{
				name: "A"
				url: "nats://127.0.0.1:%d"
				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			}]
			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-04-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-04-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfB = fmt.Sprintf(srvConfB, storeDirB, optsA.Gateway.Port)
	conf := createConfFile(t, []byte(srvConfB))
	defer removeFile(t, conf)
	srvB, optsB := RunServerWithConfig(conf)
	defer srvB.Shutdown()

	// Client connects to server A.
	cA, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsA.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	waitForOutboundGateways(t, srvB, 1, 5*time.Second)

	// Revoke the seed server cluster certificate, following servers will not be able to verify connection.
	setOCSPStatus(t, addr, "configs/certs/ocsp/server-status-request-url-02-cert.pem", ocsp.Revoked)

	// Original set of servers still can communicate to each other, even though the cert has been revoked.
	waitForOutboundGateways(t, srvA, 1, 5*time.Second)
	waitForOutboundGateways(t, srvB, 1, 5*time.Second)

	// Wait for gateway A to notice that its certificate has been revoked,
	// so that new gateways can't connect to it.
	time.Sleep(6 * time.Second)

	// Start another server against the seed server that has an invalid OCSP Staple
	srvConfC := `
		host: "127.0.0.1"
		port: -1

		server_name: "CCC"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-05-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-05-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		gateway {
			name: C
			host: "127.0.0.1"
			advertise: "127.0.0.1"
			port: -1
			gateways: [{name: "A", url: "nats://127.0.0.1:%d" }]
			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-06-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-06-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`
	srvConfC = fmt.Sprintf(srvConfC, storeDirC, optsA.Gateway.Port)
	conf = createConfFile(t, []byte(srvConfC))
	defer removeFile(t, conf)
	srvC, optsC := RunServerWithConfig(conf)
	defer srvC.Shutdown()

	cB, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsB.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	cC, err := nats.Connect(fmt.Sprintf("tls://127.0.0.1:%d", optsC.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple from server")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}

	// There should be no connectivity between the clients due to the revoked staple.
	_, err = cA.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Errorf("%v", err)
	}
	cA.Flush()
	_, err = cB.Subscribe("bar", func(m *nats.Msg) {
		m.Respond(nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	cB.Flush()

	// Gateway C was not able to mesh with Gateway A because of the revoked OCSP staple
	// so these requests to A and B should fail.
	resp, err := cC.Request("foo", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}
	// Make request to B
	resp, err = cC.Request("bar", nil, 2*time.Second)
	if err == nil {
		t.Errorf("Unexpected success, response: %+v", resp)
	}

	// Switch the certs from the seed server to new ones that are not revoked,
	// this should restart OCSP Stapling for the cluster routes.
	srvConfA = `
		host: "127.0.0.1"
		port: -1

		server_name: "AAA"

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-07-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-07-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
		store_dir: "%s"
		gateway {
			name: A
			host: "127.0.0.1"
			port: -1
			advertise: "127.0.0.1"

			tls {
				cert_file: "configs/certs/ocsp/server-status-request-url-08-cert.pem"
				key_file: "configs/certs/ocsp/server-status-request-url-08-key.pem"
				ca_file: "configs/certs/ocsp/ca-cert.pem"
				timeout: 5
			}
		}
	`

	srvConfA = fmt.Sprintf(srvConfA, storeDirA)
	if err := ioutil.WriteFile(sconfA, []byte(srvConfA), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := srvA.Reload(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)
	waitForOutboundGateways(t, srvA, 2, 5*time.Second)
	waitForOutboundGateways(t, srvB, 2, 5*time.Second)
	waitForOutboundGateways(t, srvC, 2, 5*time.Second)

	// Now clients connect to C can communicate with B and A.
	_, err = cC.Request("foo", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
	_, err = cC.Request("bar", nil, 2*time.Second)
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOCSPCustomConfig(t *testing.T) {
	const (
		caCert     = "configs/certs/ocsp/ca-cert.pem"
		caKey      = "configs/certs/ocsp/ca-key.pem"
		serverCert = "configs/certs/ocsp/server-cert.pem"
		serverKey  = "configs/certs/ocsp/server-key.pem"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	ocspURL := fmt.Sprintf("http://%s", ocspr.Addr)
	defer ocspr.Shutdown(ctx)

	var (
		errExpectedNoStaple = fmt.Errorf("expected no staple")
		errMissingStaple    = fmt.Errorf("missing OCSP Staple from server")
	)

	for _, test := range []struct {
		name      string
		config    string
		opts      []nats.Option
		err       error
		rerr      error
		configure func()
	}{
		{
			"OCSP Stapling in auto mode makes server fail to boot if status is revoked",
			`
				port: -1

				ocsp {
					mode: auto
				}

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse != nil {
							return errExpectedNoStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() { setOCSPStatus(t, ocspURL, serverCert, ocsp.Revoked) },
		},
		{
			"OCSP Stapling must staple ignored if disabled with ocsp: false",
			`
				port: -1

				ocsp: false

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse != nil {
							return errExpectedNoStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				setOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
			},
		},
		{
			"OCSP Stapling must staple ignored if disabled with ocsp mode never",
			`
				port: -1

				ocsp: { mode: never }

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse != nil {
							return errExpectedNoStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				setOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
			},
		},
		{
			"OCSP Stapling in always mode fetches a staple even if cert does not have one",
			`
				port: -1

				ocsp {
					mode: always
					url: "http://127.0.0.1:8888"
				}

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() { setOCSPStatus(t, ocspURL, serverCert, ocsp.Good) },
		},
		{
			"OCSP Stapling in must staple mode does not fetch staple if there is no must staple flag",
			`
				port: -1

				ocsp {
					mode: must
					url: "http://127.0.0.1:8888"
				}

				tls {
					cert_file: "configs/certs/ocsp/server-cert.pem"
					key_file: "configs/certs/ocsp/server-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse != nil {
							return errExpectedNoStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() { setOCSPStatus(t, ocspURL, serverCert, ocsp.Good) },
		},
		{
			"OCSP Stapling in must staple mode fetches staple if there is a must staple flag",
			`
				port: -1

				ocsp {
					mode: must
					url: "http://127.0.0.1:8888"
				}

				tls {
					cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
					key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
					ca_file: "configs/certs/ocsp/ca-cert.pem"
					timeout: 5
				}
			`,
			[]nats.Option{
				nats.Secure(&tls.Config{
					VerifyConnection: func(s tls.ConnectionState) error {
						if s.OCSPResponse == nil {
							return errMissingStaple
						}
						return nil
					},
				}),
				nats.ClientCert("./configs/certs/ocsp/client-cert.pem", "./configs/certs/ocsp/client-key.pem"),
				nats.RootCAs(caCert),
				nats.ErrorHandler(noOpErrHandler),
			},
			nil,
			nil,
			func() {
				setOCSPStatus(t, ocspURL, "configs/certs/ocsp/server-status-request-url-01-cert.pem", ocsp.Good)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.configure()
			content := test.config
			conf := createConfFile(t, []byte(content))
			defer removeFile(t, conf)
			s, opts := RunServerWithConfig(conf)
			defer s.Shutdown()

			nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port), test.opts...)
			if test.err == nil && err != nil {
				t.Errorf("Expected to connect, got %v", err)
			} else if test.err != nil && err == nil {
				t.Errorf("Expected error on connect")
			} else if test.err != nil && err != nil {
				// Error on connect was expected
				if test.err.Error() != err.Error() {
					t.Errorf("Expected error %s, got: %s", test.err, err)
				}
				return
			}
			defer nc.Close()

			nc.Subscribe("ping", func(m *nats.Msg) {
				m.Respond([]byte("pong"))
			})
			nc.Flush()

			_, err = nc.Request("ping", []byte("ping"), 250*time.Millisecond)
			if test.rerr != nil && err == nil {
				t.Errorf("Expected error getting response")
			} else if test.rerr == nil && err != nil {
				t.Errorf("Expected response")
			}
		})
	}
}

func TestOCSPCustomConfigReloadDisable(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)
	setOCSPStatus(t, addr, updatedServerCert, ocsp.Good)

	// Start with server without OCSP Stapling MustStaple
	content := `
		port: -1

		ocsp: { mode: always, url: "http://127.0.0.1:8888" }

		tls {
			cert_file: "configs/certs/ocsp/server-cert.pem"
			key_file: "configs/certs/ocsp/server-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	defer removeFile(t, conf)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Change and disable OCSP Stapling.
	content = `
		port: -1

		ocsp: { mode: never }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	if err := ioutil.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}

	// The new certificate has must staple but OCSP Stapling is disabled.
	time.Sleep(2 * time.Second)

	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("unexpected OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func TestOCSPCustomConfigReloadEnable(t *testing.T) {
	const (
		caCert            = "configs/certs/ocsp/ca-cert.pem"
		caKey             = "configs/certs/ocsp/ca-key.pem"
		serverCert        = "configs/certs/ocsp/server-cert.pem"
		updatedServerCert = "configs/certs/ocsp/server-status-request-url-01-cert.pem"
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ocspr := newOCSPResponder(t, caCert, caKey)
	defer ocspr.Shutdown(ctx)
	addr := fmt.Sprintf("http://%s", ocspr.Addr)
	setOCSPStatus(t, addr, serverCert, ocsp.Good)
	setOCSPStatus(t, addr, updatedServerCert, ocsp.Good)

	// Start with server without OCSP Stapling MustStaple
	content := `
		port: -1

		ocsp: { mode: never, url: "http://127.0.0.1:8888" }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	conf := createConfFile(t, []byte(content))
	defer removeFile(t, conf)
	s, opts := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse != nil {
					return fmt.Errorf("unexpected OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	nc.Publish("foo", []byte("hello world"))
	nc.Flush()

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()

	// Change and disable OCSP Stapling.
	content = `
		port: -1

		ocsp: { mode: always, url: "http://127.0.0.1:8888" }

		tls {
			cert_file: "configs/certs/ocsp/server-status-request-url-01-cert.pem"
			key_file: "configs/certs/ocsp/server-status-request-url-01-key.pem"
			ca_file: "configs/certs/ocsp/ca-cert.pem"
			timeout: 5
		}
	`
	if err := ioutil.WriteFile(conf, []byte(content), 0666); err != nil {
		t.Fatalf("Error writing config: %v", err)
	}
	if err := s.Reload(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)

	nc, err = nats.Connect(fmt.Sprintf("tls://localhost:%d", opts.Port),
		nats.Secure(&tls.Config{
			VerifyConnection: func(s tls.ConnectionState) error {
				if s.OCSPResponse == nil {
					return fmt.Errorf("missing OCSP Staple!")
				}
				return nil
			},
		}),
		nats.RootCAs(caCert),
		nats.ErrorHandler(noOpErrHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
}

func newOCSPResponder(t *testing.T, issuerCertPEM, issuerKeyPEM string) *http.Server {
	t.Helper()
	var mu sync.Mutex
	status := make(map[string]int)

	issuerCert := parseCertPEM(t, issuerCertPEM)
	issuerKey := parseKeyPEM(t, issuerKeyPEM)

	mux := http.NewServeMux()
	// The "/statuses/" endpoint is for directly setting a key-value pair in
	// the CA's status database.
	mux.HandleFunc("/statuses/", func(rw http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		key := r.URL.Path[len("/statuses/"):]
		switch r.Method {
		case "GET":
			mu.Lock()
			n, ok := status[key]
			if !ok {
				n = ocsp.Unknown
			}
			mu.Unlock()

			fmt.Fprintf(rw, "%s %d", key, n)
		case "POST":
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(rw, err.Error(), http.StatusBadRequest)
				return
			}

			n, err := strconv.Atoi(string(data))
			if err != nil {
				http.Error(rw, err.Error(), http.StatusBadRequest)
				return
			}

			mu.Lock()
			status[key] = n
			mu.Unlock()

			fmt.Fprintf(rw, "%s %d", key, n)
		default:
			http.Error(rw, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
	})
	// The "/" endpoint is for normal OCSP requests. This actually parses an
	// OCSP status request and signs a response with a CA. Lightly based off:
	// https://www.ietf.org/rfc/rfc2560.txt
	mux.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(rw, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		reqData, err := base64.StdEncoding.DecodeString(r.URL.Path[1:])
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}

		ocspReq, err := ocsp.ParseRequest(reqData)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}

		mu.Lock()
		n, ok := status[ocspReq.SerialNumber.String()]
		if !ok {
			n = ocsp.Unknown
		}
		mu.Unlock()

		tmpl := ocsp.Response{
			Status:       n,
			SerialNumber: ocspReq.SerialNumber,
			ThisUpdate:   time.Now(),
			NextUpdate:   time.Now().Add(4 * time.Second),
		}
		respData, err := ocsp.CreateResponse(issuerCert, issuerCert, tmpl, issuerKey)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		rw.Header().Set("Content-Type", "application/ocsp-response")
		rw.Header().Set("Content-Length", fmt.Sprint(len(respData)))

		fmt.Fprint(rw, string(respData))
	})

	srv := &http.Server{
		Addr:    "127.0.0.1:8888",
		Handler: mux,
	}
	go srv.ListenAndServe()
	time.Sleep(1 * time.Second)
	return srv
}

func setOCSPStatus(t *testing.T, ocspURL, certPEM string, status int) {
	t.Helper()

	cert := parseCertPEM(t, certPEM)

	hc := &http.Client{Timeout: 10 * time.Second}
	resp, err := hc.Post(
		fmt.Sprintf("%s/statuses/%s", ocspURL, cert.SerialNumber),
		"",
		strings.NewReader(fmt.Sprint(status)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read OCSP HTTP response body: %s", err)
	}

	if got, want := resp.Status, "200 OK"; got != want {
		t.Error(strings.TrimSpace(string(data)))
		t.Fatalf("unexpected OCSP HTTP set status, got %q, want %q", got, want)
	}
}

func parseCertPEM(t *testing.T, certPEM string) *x509.Certificate {
	t.Helper()
	block := parsePEM(t, certPEM)

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("failed to parse cert '%s': %s", certPEM, err)
	}
	return cert
}

func parseKeyPEM(t *testing.T, keyPEM string) *rsa.PrivateKey {
	t.Helper()
	block := parsePEM(t, keyPEM)

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("failed to parse ikey %s: %s", keyPEM, err)
	}
	return key
}

func parsePEM(t *testing.T, pemPath string) *pem.Block {
	t.Helper()
	data, err := ioutil.ReadFile(pemPath)
	if err != nil {
		t.Fatal(err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("failed to decode PEM %s", pemPath)
	}
	return block
}

func getOCSPStatus(s tls.ConnectionState) (*ocsp.Response, error) {
	if len(s.VerifiedChains) == 0 {
		return nil, fmt.Errorf("missing TLS verified chains")
	}
	chain := s.VerifiedChains[0]

	if got, want := len(chain), 2; got < want {
		return nil, fmt.Errorf("incomplete cert chain, got %d, want at least %d", got, want)
	}
	leaf, issuer := chain[0], chain[1]

	resp, err := ocsp.ParseResponseForCert(s.OCSPResponse, leaf, issuer)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OCSP response: %w", err)
	}
	if err := resp.CheckSignatureFrom(issuer); err != nil {
		return resp, err
	}
	return resp, nil
}

func TestOCSPTLSConfigNoLeafSet(t *testing.T) {
	o := DefaultTestOptions
	o.HTTPHost = "127.0.0.1"
	o.HTTPSPort = -1
	o.TLSConfig = &tls.Config{ServerName: "localhost"}
	cert, err := tls.LoadX509KeyPair("configs/certs/server-cert.pem", "configs/certs/server-key.pem")
	if err != nil {
		t.Fatalf("Got error reading certificates: %s", err)
	}
	o.TLSConfig.Certificates = []tls.Certificate{cert}
	s := RunServer(&o)
	s.Shutdown()
}
