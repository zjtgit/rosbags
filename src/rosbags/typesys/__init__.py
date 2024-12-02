# Copyright 2020 - 2024 Ternaris
# SPDX-License-Identifier: Apache-2.0
"""Rosbags Type System.

The type system manages ROS message types and ships all standard ROS2
distribution message types by default. The system supports custom message
types through parsers that dynamically parse custom message definitons
from different source formats.

Supported formats:
  - IDL files (subset of the standard necessary for parsing ROS2 IDL) `[1]`_
  - MSG files `[2]`_

.. _[1]: https://www.omg.org/spec/IDL/About-IDL/
.. _[2]: http://wiki.ros.org/msg

"""

from .base import TypesysError
from .deprecated import register_types
from .idl import get_types_from_idl
from .msg import get_types_from_msg
from .stores import Stores, get_typestore

__all__ = [
    'Stores',
    'TypesysError',
    'get_typestore',
    'get_types_from_idl',
    'get_types_from_msg',
    'register_types',
]
