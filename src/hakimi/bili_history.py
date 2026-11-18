"""Compatibility import for the bili_history capability implementation."""

import sys

from .modules import bili_history as implementation

sys.modules[__name__] = implementation
