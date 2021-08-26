package server

import (
	"fmt"
	"github.com/Just4Ease/graphrpc/logger"
	natsServer "github.com/nats-io/nats-server/v2/server"
	"os"
)

func setupLogger(opts *natsServer.Options) natsServer.Logger {
	var (
		slog natsServer.Logger
	)

	if opts.NoLog {
		return nil
	}

	syslog := opts.Syslog
	//if isWindowsService() && opts.LogFile == "" {
	//	// Enable syslog if no log file is specified and we're running as a
	//	// Windows service so that logs are written to the Windows event log.
	//	syslog = true
	//}

	if opts.LogFile != "" {
		slog = logger.NewFileLogger(opts.LogFile, opts.Logtime, opts.Debug, opts.Trace, true)
		if opts.LogSizeLimit > 0 {
			if l, ok := slog.(*logger.Logger); ok {
				_ = l.SetSizeLimit(opts.LogSizeLimit)
			}
		}
	} else if opts.RemoteSyslog != "" {
		slog = logger.NewRemoteSysLogger(opts.RemoteSyslog, opts.Debug, opts.Trace)
	} else if syslog {
		slog = logger.NewSysLogger(opts.Debug, opts.Trace)
	} else {
		colors := true
		// Check to see if stderr is being redirected and if so turn off color
		// Also turn off colors if we're running on Windows where os.Stderr.Stat() returns an invalid handle-error
		stat, err := os.Stderr.Stat()
		if err != nil || (stat.Mode()&os.ModeCharDevice) == 0 {
			colors = false
		}
		slog = logger.NewStdLogger(opts.Logtime, opts.Debug, opts.Trace, colors, true)
	}

	return slog
}

// PrintAndDie is exported for access in other packages.
func PrintAndDie(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

// PrintServerAndExit will print our version and exit.
func PrintServerAndExit() {
	fmt.Printf("graphrpc-server: v%d\n", 2000)
	os.Exit(0)
}
