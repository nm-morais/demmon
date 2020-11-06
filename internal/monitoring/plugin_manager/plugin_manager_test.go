package plugin_manager

import (
	"testing"
)

func TestCompileAndStorePlugin(t *testing.T) {
	pm := New(PluginManagerConfig{WorkingDir: "/tmp/plugin_dir"})
	p, err := pm.CompileAndStorePlugin("example_plugin")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if p == nil {
		t.Error("plugin is nil")
		t.FailNow()
	}

	// converted, ok := s.()

	t.Errorf("%+v", p)
	t.FailNow()
}
