test:
	go test -v -count=1 ./...

example-fullgen:
	rm example/client/gen_client.go || true
	rm example/server/generated/generated.go || true
	rm example/server/model/models_gen.go || true
	make example-genall

example-genall:
	make example-gqlgen
	make example-gqlgenc

example-gqlgen:
	cd example && go run github.com/99designs/gqlgen

example-gqlgenc:
	cd example && go run github.com/infiotinc/gqlgenc

example-test:
	cd example && go test -v -count=1 ./...

example-run-memleak:
	cd example && go run ./cmd/memleak.go

tag:
	git tag -a ${TAG} -m ${TAG}
	git push origin ${TAG}
