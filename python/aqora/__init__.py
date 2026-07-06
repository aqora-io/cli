from typing_extensions import Any, TypedDict, override

from . import _aqora
from .notebook import load
from aqora._aqora import *  # pyright: ignore[reportAssignmentType, reportWildcardImportFromLibrary]  # noqa: F403

# Imported after the wildcard import: `_provider.jobs` needs `aqora.Client`,
# which the native module binds above.
from ._provider.jobs import ProviderJob
from ._provider.results import ProviderResult


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


__doc__ = _aqora.__doc__
