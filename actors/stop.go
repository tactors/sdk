package actors

// StopCommand is a built-in command that stops the workflow loop gracefully.
type StopCommand struct {
	CommandMsg[struct{}]
}

// StopCommandAction registers a handler that returns ErrStopLoop when StopCommand runs.
func StopCommandAction[S any]() CommandAction[S] {
	return Command(func(ctx Ctx, st *S, _ StopCommand) (struct{}, error) {
		return struct{}{}, ErrStopLoop
	})
}
