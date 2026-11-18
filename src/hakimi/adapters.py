"""Compatibility import for the adapters capability implementation."""

import sys

from .modules import adapters as implementation

sys.modules[__name__] = implementation
