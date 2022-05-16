package main

import (
	"fmt"
	"github.com/borderlesshq/graphrpc/generator"
	"github.com/borderlesshq/graphrpc/libs/99designs/gqlgen/graphql"
	"github.com/gookit/color"
	"github.com/urfave/cli/v2"
	"io/ioutil"
	"log"
	"os"
)

var genCmd = &cli.Command{
	Name:  "generate",
	Usage: "generate a graphql server based on schema",
	Flags: []cli.Flag{
		&cli.BoolFlag{Name: "verbose, v", Usage: "show logs"},
		&cli.StringFlag{Name: "filename, f", Usage: "the server filename you want code to be generated into"},
	},
	Action: func(ctx *cli.Context) error {
		fileName := "server.go"

		if serverFilename := ctx.String("filename"); serverFilename != "" {
			fileName = serverFilename
		} else {
			color.Yellow.Print("⚡️ Server filename not provided, defaulting to server.go \n")
		}

		generator.GenerateGraphRPCServer(fileName)
		return nil
	},
}

func main() {
	app := cli.NewApp()
	app.Name = "graphrpcgen"
	app.Usage = genCmd.Usage
	app.Description = `
This is a library for quickly creating strictly typed graphql servers in golang.
See https://gqlgen.com/ for a getting started guide.

Note, this library will scaffold a GraphRPC Server for you.
See https://github.com/borderlesshq/graphrpc 🦾 🐙 🐉 🔦 🕸
`
	app.HideVersion = true
	app.Flags = genCmd.Flags
	app.Version = graphql.Version
	app.Before = func(context *cli.Context) error {
		if context.Bool("verbose") {
			log.SetFlags(0)
		} else {
			log.SetOutput(ioutil.Discard)
		}
		return nil
	}

	app.Action = genCmd.Action
	app.Commands = []*cli.Command{genCmd}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}
}
