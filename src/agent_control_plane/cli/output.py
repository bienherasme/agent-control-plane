"""Small stdout/stderr helpers shared by CLI commands.

Successful machine-readable output goes to stdout only; errors go to stderr only. Nothing here
ever mixes the two streams.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence

from pydantic import BaseModel


def print_json(model: BaseModel) -> None:
    sys.stdout.write(model.model_dump_json() + "\n")


def print_json_list(models: Sequence[BaseModel]) -> None:
    payload = [model.model_dump(mode="json") for model in models]
    sys.stdout.write(json.dumps(payload) + "\n")


def print_line(text: str) -> None:
    sys.stdout.write(text + "\n")


def print_error(text: str) -> None:
    sys.stderr.write(text + "\n")
