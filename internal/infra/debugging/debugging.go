package debugging

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"

	"github.com/felixge/fgprof"
)

type DebugConfig struct {
	CPUProfilePath   string // writes CPU profile to the specified path, if set
	MemProfilePath   string // writes memory profile to the specified path, if set
	TraceProfilePath string // writes trace (for `go tool trace`) to the specified path, if set
	FgprofPath       string // writes fgprof profile to the specified path, if set
}

func ConfigureDebugging(cfg DebugConfig) (stop func() error, err error) {
	cpuStop, err := configureCPUProfiling(cfg.CPUProfilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to configure CPU profiling: %w", err)
	}

	memStop, err := configureMemProfiling(cfg.MemProfilePath)
	if err != nil {
		_ = cpuStop()
		return nil, fmt.Errorf("failed to configure memory profiling: %w", err)
	}

	traceStop, err := configureTraceProfiling(cfg.TraceProfilePath)
	if err != nil {
		_ = cpuStop()
		_ = memStop()
		return nil, fmt.Errorf("failed to configure trace profiling: %w", err)
	}

	fgprofStop, err := configureFgprofProfiling(cfg.FgprofPath)
	if err != nil {
		_ = cpuStop()
		_ = memStop()
		_ = traceStop()
		return nil, fmt.Errorf("failed to configure fgprof profiling: %w", err)
	}

	return func() error {
		cpuErr := cpuStop()
		memErr := memStop()
		traceErr := traceStop()
		fgprofErr := fgprofStop()
		return errors.Join(cpuErr, memErr, traceErr, fgprofErr)
	}, nil
}

func configureCPUProfiling(path string) (stop func() error, err error) {
	if path == "" {
		return func() error { return nil }, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open CPU profile file: %w", err)
	}

	if err = pprof.StartCPUProfile(f); err != nil {
		return nil, fmt.Errorf("failed to start CPU profiling: %w", err)
	}

	return func() error {
		pprof.StopCPUProfile()
		return f.Close()
	}, nil
}

func configureMemProfiling(path string) (stop func() error, err error) {
	if path == "" {
		return func() error { return nil }, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open memory profile file: %w", err)
	}

	return func() error {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		slog.Debug("Mem stats",
			"alloc", memoryBytesValue(ms.Alloc),
			"total_alloc", memoryBytesValue(ms.TotalAlloc),
			"sys", memoryBytesValue(ms.Sys),
			"num_gc", ms.NumGC,
			"heap_alloc", memoryBytesValue(ms.HeapAlloc),
			"heap_sys", memoryBytesValue(ms.HeapSys),
			"heap_idle", memoryBytesValue(ms.HeapIdle),
			"heap_released", memoryBytesValue(ms.HeapReleased),
			"heap_inuse", memoryBytesValue(ms.HeapInuse),
			"stack_inuse", memoryBytesValue(ms.StackInuse),
			"stack_sys", memoryBytesValue(ms.StackSys),
			"mspan_inuse", memoryBytesValue(ms.MSpanInuse),
			"mspan_sys", memoryBytesValue(ms.MSpanSys),
			"buck_hash_sys", memoryBytesValue(ms.BuckHashSys),
			"gc_sys", memoryBytesValue(ms.GCSys),
			"other_sys", memoryBytesValue(ms.OtherSys),
			"mallocs", ms.Mallocs,
			"frees", ms.Frees,
			"heap_objects", ms.HeapObjects,
			"gc_cpu_fraction", fmt.Sprintf("%.2f", ms.GCCPUFraction),
		)

		if err = pprof.WriteHeapProfile(f); err != nil {
			return fmt.Errorf("failed to write memory profile: %w", err)
		}
		return f.Close()
	}, nil
}

func configureTraceProfiling(path string) (stop func() error, err error) {
	if path == "" {
		return func() error { return nil }, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open trace profile file: %w", err)
	}

	if err = trace.Start(f); err != nil {
		return nil, fmt.Errorf("failed to start trace profiling: %w", err)
	}

	return func() error {
		trace.Stop()
		return f.Close()
	}, nil
}

func configureFgprofProfiling(path string) (stop func() error, err error) {
	if path == "" {
		return func() error { return nil }, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_SYNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open fgprof profile file: %w", err)
	}

	end := fgprof.Start(f, fgprof.FormatPprof)

	return func() error {
		endErr := end()
		closeErr := f.Close()
		return errors.Join(endErr, closeErr)
	}, nil
}

type memoryBytesValue uint64

func (m memoryBytesValue) LogValue() slog.Value {
	const Kb = 1024
	const Mb = Kb * 1024

	if m < Kb {
		return slog.StringValue(fmt.Sprintf("%d bytes", m))
	}
	if m < Mb {
		return slog.StringValue(fmt.Sprintf("%d kb", m/Kb))
	}
	return slog.StringValue(fmt.Sprintf("%d mb", m/Mb))
}
