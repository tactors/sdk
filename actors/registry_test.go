package actors_test

import (
	"testing"

	"github.com/tactors/sdk/actors"
)

func TestRegistryLifecycle(t *testing.T) {
	reg := actors.NewRegistry()
	actor := actors.NewStateful("reg", func() struct{} { return struct{}{} }).
		With(actors.Command(func(ctx actors.Ctx, st *struct{}, msg struct {
			actors.CommandMsg[struct{}]
		}) (struct{}, error) {
			return struct{}{}, nil
		})).
		Build()
	reg.RegisterActor(actor)
	if got := reg.Lookup("reg"); got == nil || got.Kind != "reg" {
		t.Fatalf("lookup failed: %#v", got)
	}
	entries := reg.List()
	if len(entries) != 1 || entries[0].Kind != "reg" {
		t.Fatalf("expected one entry, got %#v", entries)
	}
	reg.Unregister("reg")
	if reg.Lookup("reg") != nil {
		t.Fatalf("expected unregister to remove entry")
	}
}

func TestDefaultRegistrySwap(t *testing.T) {
	original := actors.DefaultRegistry()
	t.Cleanup(func() {
		actors.SetDefaultRegistry(original)
	})
	override := actors.NewRegistry()
	actors.SetDefaultRegistry(override)
	actor := actors.NewStateful("swap", func() struct{} { return struct{}{} }).
		Build()
	actors.RegisterActorDescription(actor)
	if actors.LookupDescription("swap") == nil {
		t.Fatalf("lookup through default registry failed")
	}
	actors.UnregisterKind("swap")
	if len(actors.RegisteredActors()) != 0 {
		t.Fatalf("expected registry to be empty after unregister")
	}
}
