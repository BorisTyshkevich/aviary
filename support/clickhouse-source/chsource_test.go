package clickhousesource

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type fixture struct {
	t        *testing.T
	home     string
	origin   string
	checkout string
	trees    string
	rg       string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock is not installed")
	}
	base := t.TempDir()
	f := &fixture{
		t:        t,
		home:     filepath.Join(base, "home"),
		origin:   filepath.Join(base, "origin"),
		checkout: filepath.Join(base, "checkout"),
		trees:    filepath.Join(base, "trees"),
	}
	if rg, err := exec.LookPath("rg"); err == nil {
		f.rg = rg
	} else if home, err := os.UserHomeDir(); err == nil {
		if candidate := filepath.Join(home, ".local", "bin", "rg"); isExecutable(candidate) {
			f.rg = candidate
		}
	}
	if err := os.MkdirAll(f.home, 0o755); err != nil {
		t.Fatal(err)
	}
	f.git(base, "init", "-q", "-b", "main", f.origin)
	f.commit("src/a.cpp", "int alpha_token = 1;\n", "v25.3.1.1-lts")
	f.commit("src/a.cpp", "int alpha_token = 2;\n", "v25.8.1.1-lts")
	f.commit("src/b.cpp", "int beta_token = 1;\n", "v25.8.2.1-lts")
	f.git(base, "clone", "-q", f.origin, f.checkout)
	return f
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode()&0o111 != 0
}

func (f *fixture) env() []string {
	env := []string{
		"HOME=" + f.home,
		"PATH=" + os.Getenv("PATH"),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_AUTHOR_NAME=Fixture",
		"GIT_AUTHOR_EMAIL=fixture@example.com",
		"GIT_COMMITTER_NAME=Fixture",
		"GIT_COMMITTER_EMAIL=fixture@example.com",
		"CHSOURCE_UPSTREAM_ROOT=" + f.checkout,
		"CHSOURCE_ANTALYA_ROOT=" + f.checkout,
		"CHSOURCE_TREES_ROOT=" + f.trees,
	}
	if f.rg != "" {
		env = append(env, "CHSOURCE_RG_BIN="+f.rg)
	} else {
		env = append(env, "CHSOURCE_RG_BIN=/nonexistent/rg")
	}
	return env
}

func (f *fixture) git(dir string, args ...string) string {
	f.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = f.env()
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func (f *fixture) commit(path, content, tag string) string {
	f.t.Helper()
	full := filepath.Join(f.origin, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		f.t.Fatal(err)
	}
	f.git(f.origin, "add", path)
	f.git(f.origin, "commit", "-q", "-m", "change "+path)
	if tag != "" {
		f.git(f.origin, "tag", tag)
	}
	return f.git(f.origin, "rev-parse", "HEAD")
}

// addSubmodule commits vendor/lib as a submodule of origin, initializes it in the
// checkout, and puts sub_token both inside the submodule and in a superproject file.
func (f *fixture) addSubmodule() {
	f.t.Helper()
	lib := filepath.Join(filepath.Dir(f.origin), "lib")
	f.git(filepath.Dir(f.origin), "init", "-q", "-b", "main", lib)
	if err := os.WriteFile(filepath.Join(lib, "x.c"), []byte("int sub_token = 1;\n"), 0o644); err != nil {
		f.t.Fatal(err)
	}
	f.git(lib, "add", "x.c")
	f.git(lib, "commit", "-q", "-m", "lib")
	f.git(f.origin, "-c", "protocol.file.allow=always", "submodule", "add", "-q", lib, "vendor/lib")
	f.git(f.origin, "commit", "-q", "-m", "add submodule")
	f.commit("src/uses_sub.cpp", "// sub_token from vendor/lib\n", "")
	f.git(f.checkout, "pull", "-q", "--ff-only")
	f.git(f.checkout, "-c", "protocol.file.allow=always", "submodule", "update", "-q", "--init")
	if _, err := os.Stat(filepath.Join(f.checkout, "vendor", "lib", "x.c")); err != nil {
		f.t.Fatalf("submodule was not initialized: %v", err)
	}
}

func (f *fixture) hash(ref string) string {
	f.t.Helper()
	return f.git(f.checkout, "rev-parse", ref+"^{commit}")
}

func (f *fixture) run(script string, args ...string) (string, int) {
	f.t.Helper()
	path, err := filepath.Abs(script)
	if err != nil {
		f.t.Fatal(err)
	}
	cmd := exec.Command(path, args...)
	cmd.Env = f.env()
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return string(out), 0
	case errors.As(err, &exitErr):
		return string(out), exitErr.ExitCode()
	default:
		f.t.Fatalf("%s %s: %v", script, strings.Join(args, " "), err)
		return "", -1
	}
}

func (f *fixture) chsource(args ...string) string {
	f.t.Helper()
	out, code := f.run("chsource", args...)
	if code != 0 {
		f.t.Fatalf("chsource %s exited %d:\n%s", strings.Join(args, " "), code, out)
	}
	return out
}

func (f *fixture) requireRG() {
	f.t.Helper()
	if f.rg == "" {
		f.t.Skip("rg is not installed")
	}
}

func (f *fixture) worktreeList() string {
	f.t.Helper()
	return f.git(f.checkout, "worktree", "list", "--porcelain")
}

func TestSearchAtWithoutWorktreeUsesGitGrepWithPlainPaths(t *testing.T) {
	f := newFixture(t)
	hash := f.hash("v25.8.2.1-lts")

	out := f.chsource("search-at", "25.8", "alpha_token")

	if !strings.Contains(out, "Source matches at v25.8.2.1-lts ("+hash+") via git grep:") {
		t.Fatalf("missing git grep header:\n%s", out)
	}
	if !strings.Contains(out, "\nsrc/a.cpp:1:int alpha_token = 2;\n") {
		t.Fatalf("expected a match without the revision prefix:\n%s", out)
	}
	if strings.Contains(out, hash+":src/") {
		t.Fatalf("git grep revision prefix was not stripped:\n%s", out)
	}
}

func TestSearchAtGitGrepStopsAtOutputLimit(t *testing.T) {
	f := newFixture(t)
	var b strings.Builder
	for range 20000 {
		b.WriteString("repeated_token\n")
	}
	f.commit("src/many.cpp", b.String(), "")
	f.git(f.checkout, "pull", "-q", "--ff-only")

	out := f.chsource("search-at", "HEAD", "repeated_token")

	if !strings.Contains(out, "[output limit reached; narrow the query]") {
		t.Fatalf("expected the output limit marker:\n%s", out)
	}
	if lines := strings.Count(out, "src/many.cpp:"); lines != 160 {
		t.Fatalf("expected 160 bounded matches, got %d", lines)
	}
}

func TestMaterializeMakesSearchAtUseTheWorktree(t *testing.T) {
	f := newFixture(t)
	f.requireRG()
	hash := f.hash("v25.8.2.1-lts")

	out := f.chsource("materialize", "25.8")

	if !strings.Contains(out, "Worktree ready for v25.8.2.1-lts ("+hash+")") {
		t.Fatalf("unexpected materialize output:\n%s", out)
	}
	tree := filepath.Join(f.trees, "upstream", hash)
	if !strings.Contains(f.worktreeList(), "worktree "+tree+"\n") {
		t.Fatalf("worktree %s is not registered:\n%s", tree, f.worktreeList())
	}
	if _, err := os.Stat(filepath.Join(f.trees, "upstream", ".on-demand", hash)); err != nil {
		t.Fatalf("missing on-demand marker: %v", err)
	}

	out = f.chsource("search-at", "25.8", "alpha_token")
	if !strings.Contains(out, "Source matches at v25.8.2.1-lts ("+hash+") via worktree:") {
		t.Fatalf("missing worktree header:\n%s", out)
	}
	if !strings.Contains(out, "\nsrc/a.cpp:1:int alpha_token = 2;\n") {
		t.Fatalf("expected the same match as git grep:\n%s", out)
	}

	out = f.chsource("search-at", "25.8", "alpha_token", "src/b.cpp")
	if !strings.Contains(out, "via worktree:\nNo matches in tracked files.") {
		t.Fatalf("path-scoped worktree search should not match other files:\n%s", out)
	}

	// An older commit has no worktree and still uses git grep.
	out = f.chsource("search-at", "25.3", "alpha_token")
	if !strings.Contains(out, "via git grep:\nsrc/a.cpp:1:int alpha_token = 1;") {
		t.Fatalf("expected git grep at 25.3:\n%s", out)
	}
}

func TestSearchAtIgnoresWorktreeAtAnotherCommit(t *testing.T) {
	f := newFixture(t)
	f.requireRG()
	hash := f.hash("v25.8.2.1-lts")
	f.chsource("materialize", hash)
	f.git(filepath.Join(f.trees, "upstream", hash), "checkout", "-q", "--detach", "v25.3.1.1-lts")

	out := f.chsource("search-at", hash, "alpha_token")

	if !strings.Contains(out, "via git grep:\nsrc/a.cpp:1:int alpha_token = 2;") {
		t.Fatalf("a worktree whose HEAD moved must not answer for %s:\n%s", hash, out)
	}
}

func TestMaterializeRejectsUnknownRefs(t *testing.T) {
	f := newFixture(t)

	for _, ref := range []string{"no-such-tag", "../etc", "-v"} {
		out, code := f.run("chsource", "materialize", ref)
		if code != 2 {
			t.Fatalf("materialize %q exited %d:\n%s", ref, code, out)
		}
	}
	if _, err := os.Stat(filepath.Join(f.trees, "upstream")); err == nil {
		entries, _ := os.ReadDir(filepath.Join(f.trees, "upstream"))
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), ".") {
				t.Fatalf("unexpected tree %s after rejected refs", entry.Name())
			}
		}
	}
}

func TestDailyPullRefreshesSeriesWorktreesAndRetiresOldOnes(t *testing.T) {
	f := newFixture(t)
	upstream := filepath.Join(f.trees, "upstream")
	first := f.hash("v25.8.2.1-lts")
	series253 := f.hash("v25.3.1.1-lts")

	out, code := f.run("chsource-daily-pull", "upstream")
	if code != 0 {
		t.Fatalf("daily pull exited %d:\n%s", code, out)
	}
	for _, want := range []string{
		"25.8 -> v25.8.2.1-lts (" + first + ")",
		"25.3 -> v25.3.1.1-lts (" + series253 + ")",
		"Worktree refresh exit status: 0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
	for _, hash := range []string{first, series253} {
		if _, err := os.Stat(filepath.Join(upstream, hash, ".git")); err != nil {
			t.Fatalf("missing series worktree %s: %v", hash, err)
		}
	}

	// A new patch release moves 25.8; the old tree stays for one run.
	second := f.commit("src/b.cpp", "int beta_token = 2;\n", "v25.8.3.1-lts")
	out, code = f.run("chsource-daily-pull", "upstream")
	if code != 0 {
		t.Fatalf("second daily pull exited %d:\n%s", code, out)
	}
	if !strings.Contains(out, "25.8 -> v25.8.3.1-lts ("+second+")") {
		t.Fatalf("25.8 did not move to the new tag:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(upstream, second, ".git")); err != nil {
		t.Fatalf("missing new series worktree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(upstream, first, ".git")); err != nil {
		t.Fatalf("superseded worktree was removed before its grace run: %v", err)
	}
	retired, err := os.ReadFile(filepath.Join(upstream, ".retired"))
	if err != nil || strings.TrimSpace(string(retired)) != first {
		t.Fatalf("expected %s to be retired, got %q (%v)", first, retired, err)
	}

	out = f.chsource("refresh")
	if !strings.Contains(out, "Removing retired worktree "+first) {
		t.Fatalf("retired worktree was not removed:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(upstream, first)); !os.IsNotExist(err) {
		t.Fatalf("retired worktree still exists: %v", err)
	}
	list := f.worktreeList()
	if strings.Contains(list, first) || strings.Contains(list, "prunable") {
		t.Fatalf("worktree list still references the retired tree:\n%s", list)
	}
	if status := f.git(filepath.Join(upstream, series253), "status", "--porcelain", "--untracked-files=no"); status != "" {
		t.Fatalf("series worktree is modified: %s", status)
	}
}

func TestRefreshRebuildsModifiedSeriesWorktree(t *testing.T) {
	f := newFixture(t)
	hash := f.hash("v25.8.2.1-lts")
	tree := filepath.Join(f.trees, "upstream", hash)
	f.chsource("refresh")
	if err := os.WriteFile(filepath.Join(tree, "src", "b.cpp"), []byte("edited\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out := f.chsource("refresh")

	if !strings.Contains(out, "Recreating modified worktree for v25.8.2.1-lts ("+hash+")") {
		t.Fatalf("modified tree was not rebuilt:\n%s", out)
	}
	if status := f.git(tree, "status", "--porcelain", "--untracked-files=no"); status != "" {
		t.Fatalf("rebuilt worktree is still modified: %s", status)
	}
}

func TestRefreshKeepsMaterializedTreeAfterAliasMoves(t *testing.T) {
	f := newFixture(t)
	first := f.hash("v25.8.2.1-lts")
	f.chsource("refresh")
	f.chsource("materialize", first)
	f.commit("src/b.cpp", "int beta_token = 2;\n", "v25.8.3.1-lts")
	f.git(f.checkout, "pull", "-q", "--ff-only")

	f.chsource("refresh")
	out := f.chsource("refresh")

	if strings.Contains(out, "Removing retired worktree") {
		t.Fatalf("materialized tree was retired:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(f.trees, "upstream", first, ".git")); err != nil {
		t.Fatalf("materialized tree was removed: %v", err)
	}
}

func TestDailyPullAntalyaOnlyFastForwards(t *testing.T) {
	f := newFixture(t)
	next := f.commit("src/c.cpp", "int gamma_token = 1;\n", "")

	out, code := f.run("chsource-daily-pull", "antalya")

	if code != 0 {
		t.Fatalf("antalya daily pull exited %d:\n%s", code, out)
	}
	if got := f.hash("HEAD"); got != next {
		t.Fatalf("checkout HEAD is %s, want %s", got, next)
	}
	if strings.Contains(out, "Worktree refresh") {
		t.Fatalf("antalya pull should not refresh worktrees:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(f.trees, "antalya")); !os.IsNotExist(err) {
		t.Fatalf("antalya pull created trees: %v", err)
	}
	logs, _ := filepath.Glob(filepath.Join(f.checkout, "tmp", "git-pull-*.log"))
	if len(logs) != 1 {
		t.Fatalf("expected one pull log, got %v", logs)
	}
}

func TestDailyPullFailsWithoutSource(t *testing.T) {
	f := newFixture(t)

	out, code := f.run("chsource-daily-pull")

	if code != 2 || !strings.Contains(out, "Usage: chsource-daily-pull upstream|antalya") {
		t.Fatalf("unexpected result %d:\n%s", code, out)
	}
}

func TestSearchSkipsSubmoduleContent(t *testing.T) {
	f := newFixture(t)
	f.requireRG()
	f.addSubmodule()

	out := f.chsource("search", "sub_token")
	if !strings.Contains(out, "src/uses_sub.cpp:1:") || strings.Contains(out, "vendor/lib/") {
		t.Fatalf("search should match only the superproject:\n%s", out)
	}
	// A directory that contains a submodule is searched without descending into it.
	out = f.chsource("search", "sub_token", "vendor")
	if !strings.Contains(out, "No matches in tracked files.") {
		t.Fatalf("search under vendor returned submodule content:\n%s", out)
	}

	f.chsource("materialize", "HEAD")
	out = f.chsource("search-at", "HEAD", "sub_token")
	if !strings.Contains(out, "via worktree:\nsrc/uses_sub.cpp:1:") || strings.Contains(out, "vendor/lib/") {
		t.Fatalf("worktree search should match only the superproject:\n%s", out)
	}
}

func TestSubmodulePathsAreRejected(t *testing.T) {
	f := newFixture(t)
	f.addSubmodule()
	const want = "chsource: path is inside submodule vendor/lib; submodule content is out of scope"

	for _, args := range [][]string{
		{"search", "sub_token", "vendor/lib"},
		{"search", "sub_token", "vendor/lib/x.c"},
		{"read", "vendor/lib/x.c"},
		{"search-at", "HEAD", "sub_token", "vendor/lib"},
		{"search-at", "HEAD", "sub_token", "vendor/lib/x.c"},
		{"read-at", "HEAD", "vendor/lib/x.c"},
		{"read-at", "HEAD", "vendor/lib"},
	} {
		out, code := f.run("chsource", args...)
		if code != 2 || !strings.Contains(out, want) {
			t.Fatalf("%v exited %d, want the submodule refusal:\n%s", args, code, out)
		}
	}

	// Ordinary missing paths keep their own errors.
	if out, code := f.run("chsource", "read-at", "HEAD", "src/missing.cpp"); code != 2 || !strings.Contains(out, "path is not a file at that revision") {
		t.Fatalf("missing path exited %d:\n%s", code, out)
	}
}
