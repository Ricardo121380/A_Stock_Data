from __future__ import annotations

import argparse

from .config import AppConfig
from .pipeline import full_download, incremental_update, init_storage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    init_cmd = sub.add_parser("init")
    init_cmd.set_defaults(func="init")

    full_cmd = sub.add_parser("full")
    full_cmd.add_argument("--start-date", default=None)
    full_cmd.add_argument("--end-date", default=None)
    full_cmd.set_defaults(func="full")

    update_cmd = sub.add_parser("update")
    update_cmd.add_argument("--end-date", default=None)
    update_cmd.set_defaults(func="update")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    cfg = AppConfig()

    if args.command == "init":
        init_storage(cfg)
        return

    if args.command == "full":
        start_date = args.start_date or cfg.default_start_date
        end_date = args.end_date or cfg.default_end_date
        init_storage(cfg)
        full_download(cfg, start_date, end_date)
        return

    if args.command == "update":
        end_date = args.end_date or cfg.default_end_date
        init_storage(cfg)
        incremental_update(cfg, end_date)
        return


if __name__ == "__main__":
    main()
