package main

import (
	"github.com/gabriel-vasile/mimetype"
	"github.com/go-chi/chi"
	"github.com/nats-io/nats.go"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func main() {

	router := chi.NewRouter()
	ncs, err := nats.Connect("127.0.0.1:6000")
	failOnErr(err)

	router.Post("/test", func(writer http.ResponseWriter, request *http.Request) {

		body, _ := ioutil.ReadAll(request.Body)

		//form := request.Form
		//
		//headers := request.Header
		//
		//method := request.Method
		if request.Header.Get("Accept") == "" {
			request.Header.Set("Accept", mimetype.Detect(body).String())
		}

		request.Header.Set("Content-Type", mimetype.Detect(body).String())

		m, err := ncs.Request("GraphRPC.graphql", body, time.Second*1)
		if err != nil {
			log.Fatal(err)
		}

		_, _ = writer.Write(m.Data)
	})

	_ = http.ListenAndServe("127.0.0.1:20000", router)
}

func failOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
