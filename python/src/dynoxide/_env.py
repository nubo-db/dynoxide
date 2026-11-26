"""The environment variables that point AWS SDKs at a Dynoxide server."""

from __future__ import annotations

import os
from collections.abc import MutableMapping

REGION = "us-east-1"
ACCESS_KEY_ID = "dynoxide"
SECRET_ACCESS_KEY = "dynoxide"

# Names export() sets or removes, so a caller can save and restore them.
NAMES = (
    "AWS_ENDPOINT_URL_DYNAMODB",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS",
    "AWS_SESSION_TOKEN",
)


def environment(endpoint: str) -> dict[str, str]:
    """The variables for an endpoint. AWS_ENDPOINT_URL is never among them:
    it would send every AWS service to Dynoxide."""
    return {
        "AWS_ENDPOINT_URL_DYNAMODB": endpoint,
        "AWS_ACCESS_KEY_ID": ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": SECRET_ACCESS_KEY,
        "AWS_REGION": REGION,
        "AWS_DEFAULT_REGION": REGION,
        # Set to true, here or as ignore_configured_endpoint_urls in the shared
        # config file, this makes botocore ignore AWS_ENDPOINT_URL_DYNAMODB and
        # call AWS. The environment wins over the config file, so false
        # overrides both.
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
    }


def export(endpoint: str, environ: MutableMapping[str, str] | None = None) -> None:
    """Sets the variables, and removes AWS_SESSION_TOKEN so a real session
    token isn't sent along with the dummy credentials."""
    target = os.environ if environ is None else environ
    target.update(environment(endpoint))
    target.pop("AWS_SESSION_TOKEN", None)
