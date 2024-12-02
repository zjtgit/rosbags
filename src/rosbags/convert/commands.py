# Copyright 2020 - 2024 Ternaris
# SPDX-License-Identifier: Apache-2.0
"""Rosbag1 to Rosbag2 Converter."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Annotated, cast

from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from .converter import ConverterError, convert, echo

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path
    from types import ModuleType

STORENAMES = [x.name.lower() for x in Stores]


def find_obj(objpath: str) -> ModuleType:
    """Import obj from module separated by `.` or `:`."""
    if ':' in objpath:
        modpath, objname = objpath.split(':')
    else:
        modpath, objname = objpath, None

    try:
        mod = import_module(modpath)
    except ModuleNotFoundError:
        modpath, objname = modpath.rsplit('.', 1)

    mod = import_module(modpath)
    return cast('ModuleType', mod if not objname else getattr(mod, objname))


def command(
    srcs: Annotated[
        Sequence[Path],
        {
            'flags': ['--src'],
            'action': 'extend',
            'nargs': '*',
            'metavar': 'PATH',
        },
    ],
    dst: Annotated[Path, {'flags': ['--dst']}],
    dst_version: Annotated[int, {'flags': ['--dst-version']}] = 8,
    src_typestore: Annotated[
        str,
        {
            'flags': ['--src-typestore'],
            'choices': STORENAMES,
            'group': 'srcstore',
        },
    ] = 'ros2_foxy',
    src_typestore_ref: Annotated[
        str,
        {
            'flags': ['--src-typestore-ref'],
            'group': 'srcstore',
        },
    ] = '',
    dst_typestore: Annotated[
        str,
        {
            'flags': ['--dst-typestore'],
            'choices': ['copy', *STORENAMES],
            'group': 'dststore',
        },
    ] = 'copy',
    dst_typestore_ref: Annotated[
        str,
        {
            'flags': ['--dst-typestore-ref'],
            'group': 'dststore',
        },
    ] = '',
    exclude_topics: Annotated[
        Sequence[str],
        {
            'flags': ['--exclude-topic'],
            'action': 'extend',
            'nargs': '*',
            'metavar': 'TOPIC',
            'group': 'topics',
        },
    ] = (),
    include_topics: Annotated[
        Sequence[str],
        {
            'flags': ['--include-topic'],
            'action': 'extend',
            'nargs': '*',
            'metavar': 'TOPIC',
            'group': 'topics',
        },
    ] = (),
    exclude_msgtypes: Annotated[
        Sequence[str],
        {
            'flags': ['--exclude-msgtype'],
            'action': 'extend',
            'nargs': '*',
            'metavar': 'MSGTYPE',
            'group': 'msgtypes',
        },
    ] = (),
    include_msgtypes: Annotated[
        Sequence[str],
        {
            'flags': ['--include-msgtype'],
            'action': 'extend',
            'nargs': '*',
            'metavar': 'MSGTYPE',
            'group': 'msgtypes',
        },
    ] = (),
) -> int:
    """Merge, filter, and convert rosbags.

    This tool reads messages from source rosbags and writes them into a
    destination rosbag.

    When multiple source rosbags are provided, their connections are merged,
    and messages are written in correct chronological order.

    Source rosbag connections can be filtered by excluding or including based
    on topics and/or message types. Exclusions take precedence over inclusions.

    Source and destination rosbag versions do not have to match. The desired
    target version is derived from the destination filename, and conversion is
    handled automatically. When no additional parameters are specified,
    only the container type (rosbag1 <=> rosbag2) and the serialization
    format (ros1 <=> cdr) will be changed. Message definitions will be copied
    over as is from the source.

    Experimental: If the destination typestore is set to anything other than
    'copy', an automatic conversion of messages will be performed. For each
    message type conversion, it will:

        - Detect trivial field renames.
        - Drop fields that have been removed.
        - Add a default value for fields that have been added.

    Usage Examples:

        Convert bag from rosbag1 to rosbag2:
            rosbags-convert --src example.bag --dst ros2_bagdir

        Convert bag from rosbag1 to rosbag2, upgrate types to iron:
            rosbags-convert --src example.bag --dst ros2_bagdir --dst-typestore ros2_iron

        Convert bag from legacy rosbag2 (with humble types) to rosbag1:
            rosbags-convert --src ros2_bagdir --dst dst.bag --src_typestore ros2_humble

        Copy only image topics:
            rosbags-convert --src src.bag --dst dst.bag --include-topic sensor_msgs/msg/Image

    Args:
        srcs: Rosbag files to read from.
        dst: Destination path to write rosbag to.
        dst_version: Destination file format version.
        src_typestore: Source typestore name.
        src_typestore_ref: Source typestore import location.
        dst_typestore: Destination typestore name.
        dst_typestore_ref: Destination typestore import location.
        exclude_topics: Topics to exclude from conversion, even if included explicitly.
        include_topics: Topics to include in conversion, instead of all.
        exclude_msgtypes: Message types to exclude from conversion, even if included explicitly.
        include_msgtypes: Message types to include in conversion, instead of all.

    ExcGroups:
        srcstore: Default source typestore, if sources do not include types
        dststore: Destination typestore

    Groups:
        topics: Select topics to exclude or include
        msgtypes: Select message types to exclude or include using the ROS2 type name schema

    """
    if any(not x.exists() for x in srcs):
        items = [str(x) for x in srcs if not x.exists()]
        echo(f'ERROR: Source item(s) {items!r} are missing.')
        return 1

    if dst.exists():
        echo(f'Output path {str(dst)!r} exists already.')
        return 1

    if src_typestore_ref:
        obj = find_obj(src_typestore_ref)
        if not isinstance(obj, Typestore):
            echo(f'ERROR: Python reference {src_typestore_ref!r} is not a Typestore.')
            return 1
        default_typestore = cast('Typestore', obj)
    else:
        default_typestore = get_typestore(Stores(src_typestore))

    typestore: Typestore | None
    if dst_typestore_ref:
        obj = find_obj(dst_typestore_ref)
        if not isinstance(obj, Typestore):
            echo(f'ERROR: Python reference {dst_typestore_ref!r} is not a Typestore.')
            return 1
        typestore = cast('Typestore', obj)
    else:
        typestore = None if dst_typestore == 'copy' else get_typestore(Stores(dst_typestore))

    try:
        convert(
            srcs,
            dst,
            dst_version,
            default_typestore,
            typestore,
            exclude_topics,
            include_topics,
            exclude_msgtypes,
            include_msgtypes,
        )
    except ConverterError as err:
        echo(f'ERROR: {err}')
        return 1

    return 0
