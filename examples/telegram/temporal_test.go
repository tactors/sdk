package telegram_test

import (
	"testing"

	"github.com/tactors/sdk/actors"
	telegram "github.com/tactors/sdk/examples/telegram"
	"github.com/tactors/sdk/testkit"
)

func TestTelegramTemporalWorkflowQuery(t *testing.T) {
	scenario := testkit.NewActorTemporalScenario(telegram.TelegramSupportActor(), "telegram-wf", telegram.Init{
		Threads: map[string]string{"a": "neo"},
	})
	scenario.
		WhenCommand(telegram.CommandAssign{ThreadID: "b", Agent: "trinity"}).
		WhenCommand(telegram.CommandAnswer{ThreadID: "b", Body: "Resolved"}).
		WhenCommand(actors.StopCommand{}). // ensure workflow exits
		Then(func(tb testing.TB, outcome testkit.TemporalOutcome) {
			if err := outcome.Error; err != nil {
				tb.Fatalf("workflow error: %v", err)
			}
			resp, err := scenario.QueryWorkflow(telegram.QueryTranscript{ThreadID: "b"})
			if err != nil {
				tb.Fatalf("query: %v", err)
			}
			var transcript telegram.ResponseTranscript
			if err := resp.Get(&transcript); err != nil {
				tb.Fatalf("decode transcript: %v", err)
			}
			if transcript.Transcript == "" {
				tb.Fatalf("expected transcript, got empty")
			}
		}).
		Run(t)
}
