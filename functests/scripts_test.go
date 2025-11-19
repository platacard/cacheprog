package functests_test

import (
	"testing"

	"github.com/platacard/cacheprog/functests/env"
	"github.com/rogpeppe/go-internal/testscript"
)

func TestCacheprog_Functional(t *testing.T) {
	testEnv := env.NewEnv(t)
	t.Run("scripts", func(t *testing.T) {
		testscript.Run(t, testEnv.GetTestScriptParams(t))
	})
}
