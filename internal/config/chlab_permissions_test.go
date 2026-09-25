package config

import "testing"

func TestCHLabRequiresFullPreset(t *testing.T) {
	for _, name := range []string{"chlab_start", "chlab_status", "chlab_query", "chlab_stop", "chlab_node_restart"} {
		if IsToolAllowedByPreset(PermissionsPresetMinimal, name) || IsToolAllowedByPreset(PermissionsPresetStandard, name) {
			t.Errorf("%s is available without full permissions", name)
		}
		if !IsToolAllowedByPreset(PermissionsPresetFull, name) {
			t.Errorf("%s is unavailable with full permissions", name)
		}
	}
}
