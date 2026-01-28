# exceptions.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""Defines custom exceptions for flowsa"""

from __future__ import annotations

from typing import Optional


class FBANotAvailableError(Exception):
    def __init__(
        self,
        method: Optional[str] = None,
        year: Optional[int] = None,
        message: Optional[str] = None,
    ) -> None:
        if message is None:
            message = "FBA not available for requested year"
            if method:
                message = message.replace("FBA", method)
            if year:
                message = message.replace("requested year", str(year))
        self.message = message
        super().__init__(self.message)


class FlowsaMethodNotFoundError(FileNotFoundError):
    def __init__(
        self,
        method_type: Optional[str] = None,
        method: Optional[str] = None,
    ) -> None:
        message = f"{method_type} method file not found"
        if method:
            message = " ".join((message, f"for {method}"))
        self.message = message
        super().__init__(self.message)


class APIError(Exception):
    def __init__(self, api_source: str) -> None:
        message = (
            f"Key file {api_source} not found. See README for help "
            "https://github.com/cornerstone-data/bedrock/blob/main/bedrock/extract/README.md"
        )
        self.message = message
        super().__init__(self.message)


class EnvError(Exception):
    def __init__(self, key: str) -> None:
        message = (
            f"The key {key} was not found in external_paths.env. "
            f"Create key or see examples folder for help."
        )
        self.message = message
        super().__init__(self.message)


class FBSMethodConstructionError(Exception):
    """Errors in FBS methods which result in incompatible models"""

    def __init__(
        self,
        message: Optional[str] = None,
        error_type: Optional[str] = None,
    ) -> None:
        if message is None:
            message = "Error in method construction."
        if error_type == 'fxn_call':
            message = (
                "Calling functions in method files must be preceded "
                "by '!script_function:<data_source_module>'"
            )
        self.message = message
        super().__init__(self.message)
