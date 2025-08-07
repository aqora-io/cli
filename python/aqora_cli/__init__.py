from aqora_cli._aqora_cli import *  # pyright: ignore[reportAssignmentType, reportWildcardImportFromLibrary]  # noqa: F403
from typing import Any, TypedDict, override


class GraphQLError(TypedDict):
    message: str
    extensions: dict[str, Any] | None  # pyright: ignore[reportExplicitAny]


class ClientError(Exception):
    message: str
    graphql_errors: list[GraphQLError] | None

    def __init__(self, message: str, graphql_errors: list[GraphQLError] | None = None):
        super().__init__(message)
        self.message = message
        self.graphql_errors = graphql_errors

    @override
    def __str__(self):
        if self.graphql_errors:
            error_messages = ", ".join(
                error["message"] for error in self.graphql_errors
            )
            return f"{self.message}: {error_messages}"
        return self.message


__doc__ = _aqora_cli.__doc__  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType, reportUndefinedVariable]  # noqa: F405
