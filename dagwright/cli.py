import argparse
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
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

    args = parser.parse_args(argv)

    if args.command == "plan":
        from dagwright.planner import plan_command
        return plan_command(args)

    return 1


if __name__ == "__main__":
    sys.exit(main())
