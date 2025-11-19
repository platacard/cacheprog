package env

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/rogpeppe/go-internal/gotooltest"
	"github.com/rogpeppe/go-internal/testenv"
	"github.com/rogpeppe/go-internal/testscript"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Env struct {
	projectRoot     string
	stack           compose.ComposeStack // nil for short tests
	coverageDir     string
	cacheprogPath   string
	recordingServer *recordingServer
}

func NewEnv(t testing.TB) *Env {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)

	projectRoot, err := filepath.Abs(filepath.Join(file, "..", "..", ".."))
	require.NoError(t, err)

	env := &Env{
		projectRoot:     projectRoot,
		recordingServer: newRecordingServer(),
	}

	// compile cacheprog locally once
	binDir := t.TempDir()

	env.cacheprogPath = filepath.Join(binDir, "cacheprog")
	if runtime.GOOS == "windows" {
		env.cacheprogPath += ".exe"
	}

	// compile cacheprog locally, with race and coverage enabled
	compiler := testenv.GoToolPath(t)
	args := []string{
		"build",
		"-C", projectRoot,
		"-o", env.cacheprogPath,
	}
	if RaceEnabled {
		args = append(args, "-race")
	}
	if covDir := os.Getenv("GOCOVERDIR"); covDir != "" {
		args = append(args, "-cover")
		env.coverageDir = covDir
	}
	args = append(args, "./cmd/cacheprog")
	cmd := testenv.CleanCmdEnv(exec.Command(compiler, args...))
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to build cacheprog: %s", string(output))

	if testing.Short() {
		return env
	}

	env.runInProjectRoot(t, func() {
		t.Helper()

		var err error
		composeStack, err := compose.NewDockerComposeWith(
			compose.WithStackFiles(filepath.Join("deployments", "compose", "docker-compose.yml")),
			compose.WithLogger(&testLogger{t: t}),
		)
		require.NoError(t, err)

		env.stack = composeStack.
			WithOsEnv().
			WaitForService("minio", wait.ForHealthCheck()). // wait for minio to be ready
			WaitForService("mc", wait.ForHealthCheck())     // wait for bucket to be created

		require.NoError(t, env.stack.Up(t.Context(), compose.WithRecreate("force"), compose.RemoveOrphans(true)))

		t.Cleanup(func() {
			_ = env.stack.Down(context.Background(), compose.RemoveOrphans(true))
		})
	})

	return env
}

func (e *Env) getServiceExposedAddress(ctx context.Context, serviceName string, proto, port string) (string, error) {
	container, err := e.stack.ServiceContainer(ctx, serviceName)
	if err != nil {
		return "", fmt.Errorf("failed to get service container: %w", err)
	}

	portObj, err := nat.NewPort(proto, port)
	if err != nil {
		return "", fmt.Errorf("failed to create port: %w", err)
	}

	endpoint, err := container.PortEndpoint(ctx, portObj, "")
	if err != nil {
		return "", fmt.Errorf("failed to get endpoint: %w", err)
	}

	return endpoint, nil
}

func (e *Env) GetTestScriptParams(t testing.TB) testscript.Params {
	t.Helper()
	p := testscript.Params{
		Dir:         "testscripts",
		WorkdirRoot: t.TempDir(),
		Setup: func(env *testscript.Env) error {
			if e.coverageDir != "" {
				env.Setenv("GOCOVERDIR", e.coverageDir)
			}

			return nil
		},
		Cmds: map[string]func(ts *testscript.TestScript, neg bool, args []string){
			// 1st arg is a name of the env var
			// 2nd arg (optional) is a stderr capture file, shell required
			"set-cacheprog": func(ts *testscript.TestScript, neg bool, args []string) {
				if len(args) < 1 {
					ts.Fatalf("set-cacheprog expects at least 1 argument, got %d", len(args))
				}

				if len(args) == 1 {
					ts.Setenv(args[0], e.cacheprogPath)
				} else {
					// TODO: maybe we should add log redirection as option to cacheprog itself
					ts.Setenv(args[0], fmt.Sprintf("%s --log-output=%q", e.cacheprogPath, args[1]))
				}
			},
			// 1st arg is a name of the env var
			"allocate-port": func(ts *testscript.TestScript, neg bool, args []string) {
				if len(args) < 1 {
					ts.Fatalf("allocate-port expects at least 1 argument, got %d", len(args))
				}

				listener, err := net.Listen("tcp", ":0")
				ts.Check(err)
				ts.Setenv(args[0], strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
				ts.Check(listener.Close())
			},
			"recording-server":  e.recordingServerCmd,
			"install-go-binary": e.installGoBinaryCmd,
		},
		Condition: func(cond string) (bool, error) {
			switch cond {
			case "cgo":
				return testenv.HasCGO(), nil
			case "race":
				return RaceEnabled, nil
			default:
				return false, fmt.Errorf("unknown condition: %s", cond)
			}
		},
	}
	require.NoError(t, gotooltest.Setup(&p))

	e.configureMinio(&p)

	return p
}

func (e *Env) configureMinio(params *testscript.Params) {
	if e.stack == nil {
		return
	}
	origSetup := params.Setup
	params.Setup = func(env *testscript.Env) error {
		err := origSetup(env)
		if err != nil {
			return err
		}

		minioAddress, err := e.getServiceExposedAddress(context.Background(), "toxiproxy", "tcp", "9000")
		if err != nil {
			return fmt.Errorf("failed to get minio address: %w", err)
		}

		// configuration for s3 backend to avoid repetition in scripts
		env.Setenv("CACHEPROG_S3_ENDPOINT", (&url.URL{
			Scheme: "minio+http",
			Host:   minioAddress,
		}).String())
		env.Setenv("CACHEPROG_S3_BUCKET", "files-bucket")
		env.Setenv("CACHEPROG_S3_FORCE_PATH_STYLE", "true")
		env.Setenv("CACHEPROG_S3_ACCESS_KEY_ID", cmp.Or(os.Getenv("MINIO_ROOT_USER"), "minioadmin"))
		env.Setenv("CACHEPROG_S3_ACCESS_KEY_SECRET", cmp.Or(os.Getenv("MINIO_ROOT_PASSWORD"), "minioadmin"))
		env.Setenv("CACHEPROG_S3_SESSION_TOKEN", "")
		env.Setenv("MINIO_ALIAS", "myminio") // defined in docker-compose.yml

		return nil
	}

	// propagate minio client to the scripts
	params.Cmds["mc"] = e.mcCmd
}

func (e *Env) mcCmd(ts *testscript.TestScript, neg bool, args []string) {
	mcContainer, err := e.stack.ServiceContainer(context.Background(), "mc")
	ts.Check(err)

	exitCode, out, err := mcContainer.Exec(context.Background(),
		append([]string{"mc"}, args...),
	)
	ts.Check(err)

	_, err = stdcopy.StdCopy(ts.Stdout(), ts.Stderr(), out)
	ts.Check(err)

	if exitCode != 0 && !neg {
		ts.Fatalf("mc command failed with exit code %d", exitCode)
	}

	if exitCode == 0 && neg {
		ts.Fatalf("mc command succeeded, but expected to fail")
	}
}

func (e *Env) recordingServerCmd(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) < 1 {
		ts.Fatalf("command expects at least 1 argument, got %d", len(args))
	}

	cmd := args[0]
	if cmd == "start" {
		if len(args) < 2 {
			ts.Fatalf("command start expects at least 2 arguments, got %d", len(args))
		}

		addressEnvVar := args[1]

		server := httptest.NewServer(e.recordingServer)
		ts.Defer(func() {
			server.Close()
		})

		ts.Setenv(addressEnvVar, server.URL)
		return
	}

	if cmd != "get" {
		ts.Fatalf("unknown command: %s", cmd)
		return
	}

	if len(args) < 3 {
		ts.Fatalf("command get expects at least 3 arguments, got %d", len(args))
	}

	recordKey := args[1]
	whatToGet := args[2]

	request, ok := e.recordingServer.GetRequest(recordKey)
	if !ok && !neg {
		ts.Fatalf("request not found for key: %s", recordKey)
		return
	}
	if !ok && neg {
		ts.Fatalf("request found for key: %s", recordKey)
		return
	}

	var err error

	switch whatToGet {
	case "body":
		_, err = ts.Stdout().Write(request.Body)
	case "method":
		_, err = io.WriteString(ts.Stdout(), request.Method)
	case "path":
		_, err = io.WriteString(ts.Stdout(), request.Path)
	case "query":
		_, err = io.WriteString(ts.Stdout(), request.Query.Encode())
	case "headers":
		err = request.Headers.Write(ts.Stdout())
	default:
		ts.Fatalf("unknown what to get: %s", whatToGet)
	}

	ts.Check(err)
}

func (e *Env) installGoBinaryCmd(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) < 2 {
		ts.Fatalf("command expects at least 2 argument, got %d", len(args))
	}

	importPath := args[0]
	targetDir := ts.MkAbs(args[1])

	compiler, err := testenv.GoTool()
	ts.Check(err)

	compilerArgs := []string{"install"}
	if testing.Verbose() {
		compilerArgs = append(compilerArgs, "-v")
	}
	compilerArgs = append(compilerArgs, importPath)

	cmd := testenv.CleanCmdEnv(exec.Command(compiler, compilerArgs...))
	// forcefully disable cgo
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, "CGO_ENABLED=") {
			continue
		}
		cmd.Env = append(cmd.Env, env)
	}
	cmd.Env = append(cmd.Env, "GOBIN="+targetDir, "CGO_ENABLED=0")
	cmd.Stdout = ts.Stdout()
	cmd.Stderr = ts.Stderr()

	ts.Check(cmd.Run())
}

type testLogger struct {
	t testing.TB
}

func (l *testLogger) Printf(format string, v ...any) {
	l.t.Helper()
	l.t.Logf(format, v...)
}

func (e *Env) runInProjectRoot(t testing.TB, fn func()) {
	t.Helper()

	originalDir, err := os.Getwd()
	require.NoError(t, err, "failed to get current directory")

	err = os.Chdir(e.projectRoot)
	require.NoError(t, err, "failed to change to project root")
	// test if it is real project root, must contain go.mod
	_, err = os.Stat("go.mod")
	require.NoError(t, err, "go.mod not found in project root")

	defer func() {
		t.Helper()

		require.NoError(t, os.Chdir(originalDir), "failed to change back to original directory")
	}()

	fn()
}
