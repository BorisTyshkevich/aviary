package agent

import "context"

type toolPolicyKey struct{}

// WithToolPolicy binds effective permissions to a turn, including indirect calls.
// Only trusted runtime code supplies the policy; tool arguments cannot replace it.
func WithToolPolicy(ctx context.Context, allowed func(string) bool) context.Context {
	if allowed == nil {
		return ctx
	}
	if parent, ok := ctx.Value(toolPolicyKey{}).(func(string) bool); ok && parent != nil {
		child := allowed
		allowed = func(name string) bool { return parent(name) && child(name) }
	}
	return context.WithValue(ctx, toolPolicyKey{}, allowed)
}

// ToolPolicyAllows returns the turn's decision and whether a turn policy exists.
func ToolPolicyAllows(ctx context.Context, name string) (allowed, present bool) {
	policy, ok := ctx.Value(toolPolicyKey{}).(func(string) bool)
	if !ok || policy == nil {
		return true, false
	}
	return policy(name), true
}
