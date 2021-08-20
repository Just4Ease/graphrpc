// +build mage

package main

import (
	genSchema "github.com/99designs/gqlgen/cmd"
)

// Default target to run when none is specified
// If not set, running mage will list available targets
var Default = GenSchema

func GenSchema() error {
	genSchema.Execute()
	return nil
}
