package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/KimMachineGun/automemlimit/memlimit"
	arg "github.com/alexflint/go-arg"

	"github.com/platacard/cacheprog/internal/infra/debugging"
	"github.com/platacard/cacheprog/internal/infra/logging"
	"github.com/platacard/cacheprog/internal/infra/metrics"
)

type Args struct {
	version string

	DebugArgs
	LoggingArgs

	CacheprogApp *CacheprogAppArgs `arg:"subcommand:direct" help:"[default] run as GOCACHEPROG program"`
	ProxyApp     *ProxyAppArgs     `arg:"subcommand:proxy" help:"run as storage and optionally metrics proxy"`

	UseVMHistograms bool `arg:"--use-vm-histograms,env:USE_VM_HISTOGRAMS" placeholder:"true/false" help:"Use VictoriaMetrics-style histograms instead of Prometheus-style histograms"`
}

type DebugArgs struct {
	CPUProfilePath   string `arg:"--cpu-profile,env:CPU_PROFILE_PATH" placeholder:"PATH" help:"Path to write CPU profile"`
	MemProfilePath   string `arg:"--mem-profile,env:MEM_PROFILE_PATH" placeholder:"PATH" help:"Path to write memory profile"`
	TraceProfilePath string `arg:"--trace-profile,env:TRACE_PROFILE_PATH" placeholder:"PATH" help:"Path to write trace profile"`
	FgprofPath       string `arg:"--fgprof,env:FGPROF_PATH" placeholder:"PATH" help:"Path to write fgprof profile"`
}

func (d *DebugArgs) configureDebugging() (func() error, error) {
	return debugging.ConfigureDebugging(debugging.DebugConfig{
		CPUProfilePath:   d.CPUProfilePath,
		MemProfilePath:   d.MemProfilePath,
		TraceProfilePath: d.TraceProfilePath,
		FgprofPath:       d.FgprofPath,
	})
}

type LoggingArgs struct {
	Level  slog.Level `arg:"--log-level,env:LOG_LEVEL" placeholder:"LEVEL" default:"INFO" help:"Logging level. Available: DEBUG, INFO, WARN, ERROR"`
	Output string     `arg:"--log-output,env:LOG_OUTPUT" placeholder:"PATH" help:"Logging output. Path to file, stderr if not provided"`
}

func (l *LoggingArgs) createLogger() (*slog.Logger, func() error, error) {
	if l.Output == "" {
		return logging.ConfigureLogger(logging.Config{
			Level:  l.Level,
			Output: os.Stderr,
		}), func() error { return nil }, nil
	}

	outFile, err := os.OpenFile(l.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open log output file: %w", err)
	}

	return logging.ConfigureLogger(logging.Config{
			Level:  l.Level,
			Output: outFile,
		}),
		func() error {
			return errors.Join(outFile.Sync(), outFile.Close())
		},
		nil
}

func (a *Args) Run(ctx context.Context) error {
	metrics.EnableVMHistograms(a.UseVMHistograms)

	logger, stopLogger, err := a.createLogger()
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	defer stopLogger() //nolint:errcheck // error handling is not important here

	slog.SetDefault(logger)

	if _, err := memlimit.SetGoMemLimitWithOpts(memlimit.WithLogger(logger)); err != nil {
		slog.WarnContext(ctx, "Failed to configure memory limit", logging.Error(err))
	}

	stopDebugging, err := a.configureDebugging()
	if err != nil {
		return fmt.Errorf("failed to configure debugging: %w", err)
	}
	defer stopDebugging() //nolint:errcheck // error handling is not important here

	switch {
	case a.CacheprogApp != nil:
		return a.CacheprogApp.Run(ctx)
	case a.ProxyApp != nil:
		return a.ProxyApp.Run(ctx)
	default:
		return fmt.Errorf("no command provided")
	}
}

func (a *Args) Version() string {
	var sb strings.Builder
	sb.WriteString("cacheprog version ")
	sb.WriteString(a.version)

	if runtimeVersion := runtime.Version(); runtimeVersion != "" {
		_, _ = fmt.Fprintf(&sb, " built with %s", runtimeVersion)
	}

	var vcsType, vcsRevision, vcsTime string
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range buildInfo.Settings {
			if setting.Key == "vcs" {
				vcsType = setting.Value
			}
			if setting.Key == "vcs.revision" {
				vcsRevision = setting.Value
			}
			if setting.Key == "vcs.time" {
				vcsTime = setting.Value
			}
		}
	}

	// for "git" vcs clip revision to 7 characters (default git behavior)
	if vcsRevision != "" {
		if vcsType == "git" {
			vcsRevision = vcsRevision[:7]
		}
		_, _ = fmt.Fprintf(&sb, " from %s", vcsRevision)
	}
	if vcsTime != "" {
		_, _ = fmt.Fprintf(&sb, " on %s", vcsTime)
	}
	return sb.String()
}

func RunApp(ctx context.Context, version string, args ...string) int {
	appArgs := Args{version: version}

	parser, err := arg.NewParser(arg.Config{
		EnvPrefix: "CACHEPROG_",
		Out:       os.Stderr,
	}, &appArgs)
	if err != nil {
		fmt.Println(err)
		return 2
	}

	// unfortunately, arg package does not support 'root' subcommand, so do following:
	// if we didn't found any subcommand name in args, then we need to run 'direct' subcommand
	// by forcefully appending it to args and parse again
	err = parser.Parse(args)
	if err == nil {
		subcommandNames := parser.SubcommandNames()
		subcommandNamesMap := make(map[string]struct{}, len(subcommandNames))
		for _, name := range subcommandNames {
			subcommandNamesMap[name] = struct{}{}
		}
		if !slices.ContainsFunc(args, func(arg string) bool {
			_, ok := subcommandNamesMap[arg]
			return ok
		}) {
			args = append([]string{"direct"}, args...)
		}

		err = parser.Parse(args)
	}

	switch {
	case errors.Is(err, nil):
		// pass
	case errors.Is(err, arg.ErrHelp):
		parser.WriteHelp(os.Stderr)
		return 0
	case errors.Is(err, arg.ErrVersion):
		fmt.Printf("%s\n", appArgs.Version())
		return 0
	default:
		fmt.Fprintln(os.Stderr, err)
		parser.WriteHelp(os.Stderr)
		return 1
	}

	if err = appArgs.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	return 0
}

func urlOrEmpty(u *url.URL) string {
	if u == nil {
		return ""
	}
	return u.String()
}

type httpHeader map[string]string

func (h *httpHeader) UnmarshalText(text []byte) error {
	if *h == nil {
		*h = make(map[string]string)
	}
	k, v, found := bytes.Cut(text, []byte(":"))
	if !found {
		return fmt.Errorf("invalid http header: %s", string(text))
	}
	(*h)[string(k)] = string(v)
	return nil
}

func headerValuesToHTTP(hs []httpHeader) http.Header {
	hh := make(http.Header)
	for _, h := range hs {
		for k, v := range h {
			hh.Add(k, v)
		}
	}
	return hh
}
