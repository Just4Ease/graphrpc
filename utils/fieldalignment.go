package utils

import (
	"fmt"
	"os/exec"
)

func FixFieldAlignment(pathToFile string) {
	// See the following articles on memory management & layouts.
	// https://itnext.io/structure-size-optimization-in-golang-alignment-padding-more-effective-memory-layout-linters-fffdcba27c61
	// https://www.reddit.com/r/golang/comments/ljn784/structure_size_optimization_in_golang/
	// https://www.meziantou.net/optimize-struct-performances-using-structlayout.htm
	cmd := exec.Command("go", "run", "golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment", "-fix", fmt.Sprintf("./%s", pathToFile))
	_, _ = cmd.Output()
}
