import argparse
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    # Windows consoles default to cp1252; force utf-8 so Unicode in
    # plan output renders cleanly when piped to a terminal.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(
        prog="dagwright",
        description="Architectural change planner for analytics engineers.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="Generate ranked change plans for a spec.")
    plan.add_argument("--spec", type=Path, required=True, help="Path to dagwright-spec YAML.")
    plan.add_argument("--manifest", type=Path, required=True, help="Path to dbt manifest.json.")
    plan.add_argument("--bi", type=Path, default=None, help="Path to BI consumer graph JSON (e.g. metabase.json).")
    plan.add_argument("--format", choices=["json", "markdown", "both"], default="both", help="Output format.")
    plan.add_argument("--top", type=int, default=3, help="Number of top-ranked plans to emit.")

    watch = sub.add_parser(
        "watch",
        help="Re-run plan whenever the spec, manifest, or BI graph changes.",
    )
    watch.add_argument("--spec", type=Path, required=True, help="Path to dagwright-spec YAML.")
    watch.add_argument("--manifest", type=Path, required=True, help="Path to dbt manifest.json.")
    watch.add_argument("--bi", type=Path, default=None, help="Path to BI consumer graph JSON.")
    watch.add_argument(
        "--format", choices=["json", "markdown", "both"], default="markdown",
        help="Output format. Defaults to markdown for live readability.",
    )
    watch.add_argument("--top", type=int, default=3, help="Number of top-ranked plans to emit.")

    args = parser.parse_args(argv)

    if args.command == "plan":
        from dagwright.planner import plan_command
        return plan_command(args)

    if args.command == "watch":
        from dagwright.watch import watch_command
        return watch_command(args)

    return 1


if __name__ == "__main__":
    sys.exit(main())
