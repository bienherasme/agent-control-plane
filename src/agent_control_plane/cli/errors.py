"""Internal CLI error taxonomy, kept small and mapped to a handful of process exit codes.

These wrap known boundary failures so main() can report a concise message on stderr and exit
with a distinct code, without ever hiding an unexpected programming error behind a generic
"operation failed" message: anything not caught here propagates as a real traceback.
"""

from __future__ import annotations


class CliInputError(Exception):
    """Invalid user input or configuration: bad JSON, an invalid model, a naive datetime."""


class CliOperationalError(Exception):
    """A known operational failure: storage conflict, persistence error, comparison failure."""


class CliNotFoundError(Exception):
    """The requested execution does not exist in the store."""
