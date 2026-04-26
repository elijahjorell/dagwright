"""Watch mode: re-run the planner on every save of the spec or manifest.

The point: turn the iteration loop the project sells (sub-second
plan generation, zero token cost) into a live UX. Without this, the
AE saves their spec and re-runs the CLI by hand. With it, save = new
plan, with no command in between. See CHARTER.md "What this changes
about AE workflows" for the framing.
"""

import time
from pathlib import Path

from watchfiles import watch

from dagwright.loaders import SpecError


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

    _run_once(args, header="[initial run]")

    try:
        for changes in watch(*str_paths):
            changed = sorted({Path(p).name for _, p in changes})
            header = f"[changed: {', '.join(changed)}]"
            _run_once(args, header=header)
    except KeyboardInterrupt:
        print("\n[dagwright watch] stopped", flush=True)
        return 0


def _run_once(args, header: str) -> None:
    # Late import to avoid pulling planner machinery at module load
    # (also keeps a circular-import door closed).
    from dagwright.planner import plan_command

    print(f"\n{header}", flush=True)
    t0 = time.perf_counter()
    try:
        plan_command(args)
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
        # YAML mid-edit) without exiting. Re-raises would defeat the
        # iteration-loop UX.
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(
            f"\n[error] {type(e).__name__} after {elapsed_ms:.0f} ms: {e}\n",
            flush=True,
        )
        return
    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"\n[completed in {elapsed_ms:.0f} ms]\n", flush=True)
