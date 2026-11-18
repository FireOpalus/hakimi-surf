"""Compatibility import for the onebot_transport capability implementation."""

import sys

from .modules import onebot_transport as implementation

sys.modules[__name__] = implementation
