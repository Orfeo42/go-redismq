//go:build ruleguard

package gorules

import (
	"github.com/quasilyte/go-ruleguard/dsl"
)

// logAttrsThenPropagate flags the "log once at the boundary" convention being
// broken: an error already logged via logAttrs at LevelError being returned
// again, so the eventual caller's own boundary log (or this same call site,
// on a future edit) double-logs the identical failure.
func logAttrsThenPropagate(m dsl.Matcher) {
	m.Match(
		`logAttrs($ctx, slog.LevelError, $msg, $*_); $*_; return $err`,
		`logAttrs($ctx, slog.LevelError, $msg, $*_); $*_; return $*_, $err`,
	).Where(m["err"].Type.Is("error") &&
		!m["err"].Text.Matches(`^nil$`)).
		Report(`error logged via logAttrs at LevelError and then propagated (double logging) — this call site is the boundary: log once and absorb (bare return), or drop the log here and let the caller's own boundary log it`)
}

// logAttrsThenPropagateWrapped is the same double-logging shape, but the
// return re-wraps the cause with fmt.Errorf instead of returning it bare.
func logAttrsThenPropagateWrapped(m dsl.Matcher) {
	m.Match(
		`logAttrs($ctx, slog.LevelError, $msg, $*_); $*_; return fmt.Errorf($*_)`,
		`logAttrs($ctx, slog.LevelError, $msg, $*_); $*_; return $*_, fmt.Errorf($*_)`,
	).Report(`error logged via logAttrs at LevelError and then propagated as a wrapped error (double logging) — this call site is the boundary: log once and absorb (bare return), or drop the log here and let the caller's own boundary log it`)
}
