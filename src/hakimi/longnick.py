"""Compatibility import for the longnick capability implementation."""

import sys

from .modules import longnick as implementation

sys.modules[__name__] = implementation
