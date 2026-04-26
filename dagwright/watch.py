"""Watch mode: re-run the planner on every save of the spec or manifest.

The point: turn the iteration loop the project sells (sub-second
plan generation, zero token cost) into a live UX. Without this, the
AE saves their spec and re-runs the CLI by hand. With it, save = new
plan, with no command in between. See CHARTER.md "How the workflow
actually runs" for the framing.

Plan diff is rendered between consecutive runs so the AE doesn't
have to re-read the full plan to spot what changed — under the
compiler framing, the AE consumes the plan, not the spec, so the
plan-side delta is the only signal of "what did my last NL request
actually change?".
"""

import time
from pathlib import Path

from watchfiles import watch

from dagwright.diff import diff_dc_plans
from dagwright.loaders import SpecError
from dagwright.state import DefinitionalChange


def watch_command(args) -> int:
    paths = [args.spec, args.manifest]
    if args.bi:
        paths.append(args.bi)

    missing = [p for p in paths if not Path(p).is_file()]
    if missing:
        for p in missing:
            print(f"[dagwright watch] error: {p} does not exist", flush=True)
        return 1

    str_paths = [str(p) for p in paths]
    print(f"[dagwright watch] watching {len(paths)} file(s):", flush=True)
    for p in paths:
        print(f"  - {p}", flush=True)
    print(flush=True)

    state = _WatchState()
    _run_once(args, state, header="[initial run]")

    try:
        for changes in watch(*str_paths):
            changed = sorted({Path(p).name for _, p in changes})
            header = f"[changed: {', '.join(changed)}]"
            _run_once(args, state, header=header)
    except KeyboardInterrupt:
        print("\n[dagwright watch] stopped", flush=True)
        return 0


class _WatchState:
    """Holds the previous run's plans so diff can compare. Kept on
    the stack of watch_command — does not persist across CLI
    invocations."""

    def __init__(self):
        self.previous_plans: list = []
        self.previous_spec_kind: type | None = None


def _run_once(args, state: _WatchState, header: str) -> None:
    # Late import to keep the module light at import time.
    from dagwright.planner import run_plan, render_plan_output

    print(f"\n{header}", flush=True)
    t0 = time.perf_counter()
    try:
        spec, plans, rejections = run_plan(args)
    except SpecError as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(
            f"\n[error] SpecError after {elapsed_ms:.0f} ms: {e}\n",
            flush=True,
        )
        return
    except FileNotFoundError as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(
            f"\n[error] FileNotFoundError after {elapsed_ms:.0f} ms: {e}\n",
            flush=True,
        )
        return
    except Exception as e:
        # Catch-all so the watcher survives a transient error (e.g.
        # YAML mid-edit) without exiting.
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(
            f"\n[error] {type(e).__name__} after {elapsed_ms:.0f} ms: {e}\n",
            flush=True,
        )
        return

    # Diff (if applicable). Only DefinitionalChange has a diff
    # implementation in v0; metric_request plans pass through.
    if isinstance(spec, DefinitionalChange) and isinstance(state.previous_spec_kind, type) and state.previous_spec_kind is DefinitionalChange:
        diff_md = diff_dc_plans(state.previous_plans, plans)
        if diff_md:
            print("\n### Diff vs previous run\n", flush=True)
            print(diff_md, flush=True)
            print(flush=True)

    render_plan_output(args, spec, plans, rejections)

    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"\n[completed in {elapsed_ms:.0f} ms]\n", flush=True)

    state.previous_plans = plans
    state.previous_spec_kind = type(spec)
