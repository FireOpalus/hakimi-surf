"""Compatibility import for the proactive_dm capability implementation."""

import sys

from .modules import proactive_dm as implementation

sys.modules[__name__] = implementation
