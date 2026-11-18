"""Compatibility import for the bilibili capability implementation."""

import sys

from .modules import bilibili as implementation

sys.modules[__name__] = implementation
