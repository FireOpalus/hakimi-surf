"""Compatibility import for the bili_watcher capability implementation."""

import sys

from .modules import bili_watcher as implementation

sys.modules[__name__] = implementation
