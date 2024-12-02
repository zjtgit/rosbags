# Copyright 2020 - 2024 Ternaris
# SPDX-License-Identifier: Apache-2.0
#
# THIS FILE IS GENERATED, DO NOT EDIT
"""Message type definitions."""

# ruff: noqa: N801,N814,N816,TCH004

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from rosbags.interfaces import Nodetype as T

from . import ros2_galactic as base

if TYPE_CHECKING:
    from typing import ClassVar

    import numpy as np

    from rosbags.interfaces.typing import Typesdict


action_msgs__msg__GoalInfo = base.action_msgs__msg__GoalInfo
action_msgs__msg__GoalStatus = base.action_msgs__msg__GoalStatus
action_msgs__msg__GoalStatusArray = base.action_msgs__msg__GoalStatusArray
actionlib_msgs__msg__GoalID = base.actionlib_msgs__msg__GoalID
actionlib_msgs__msg__GoalStatus = base.actionlib_msgs__msg__GoalStatus
actionlib_msgs__msg__GoalStatusArray = base.actionlib_msgs__msg__GoalStatusArray
builtin_interfaces__msg__Duration = base.builtin_interfaces__msg__Duration
builtin_interfaces__msg__Time = base.builtin_interfaces__msg__Time
diagnostic_msgs__msg__DiagnosticArray = base.diagnostic_msgs__msg__DiagnosticArray
diagnostic_msgs__msg__DiagnosticStatus = base.diagnostic_msgs__msg__DiagnosticStatus
diagnostic_msgs__msg__KeyValue = base.diagnostic_msgs__msg__KeyValue
geometry_msgs__msg__Accel = base.geometry_msgs__msg__Accel
geometry_msgs__msg__AccelStamped = base.geometry_msgs__msg__AccelStamped
geometry_msgs__msg__AccelWithCovariance = base.geometry_msgs__msg__AccelWithCovariance
geometry_msgs__msg__AccelWithCovarianceStamped = base.geometry_msgs__msg__AccelWithCovarianceStamped
geometry_msgs__msg__Inertia = base.geometry_msgs__msg__Inertia
geometry_msgs__msg__InertiaStamped = base.geometry_msgs__msg__InertiaStamped
geometry_msgs__msg__Point = base.geometry_msgs__msg__Point
geometry_msgs__msg__Point32 = base.geometry_msgs__msg__Point32
geometry_msgs__msg__PointStamped = base.geometry_msgs__msg__PointStamped
geometry_msgs__msg__Polygon = base.geometry_msgs__msg__Polygon
geometry_msgs__msg__PolygonStamped = base.geometry_msgs__msg__PolygonStamped
geometry_msgs__msg__Pose = base.geometry_msgs__msg__Pose
geometry_msgs__msg__Pose2D = base.geometry_msgs__msg__Pose2D
geometry_msgs__msg__PoseArray = base.geometry_msgs__msg__PoseArray
geometry_msgs__msg__PoseStamped = base.geometry_msgs__msg__PoseStamped
geometry_msgs__msg__PoseWithCovariance = base.geometry_msgs__msg__PoseWithCovariance
geometry_msgs__msg__PoseWithCovarianceStamped = base.geometry_msgs__msg__PoseWithCovarianceStamped
geometry_msgs__msg__Quaternion = base.geometry_msgs__msg__Quaternion
geometry_msgs__msg__QuaternionStamped = base.geometry_msgs__msg__QuaternionStamped
geometry_msgs__msg__Transform = base.geometry_msgs__msg__Transform
geometry_msgs__msg__TransformStamped = base.geometry_msgs__msg__TransformStamped
geometry_msgs__msg__Twist = base.geometry_msgs__msg__Twist
geometry_msgs__msg__TwistStamped = base.geometry_msgs__msg__TwistStamped
geometry_msgs__msg__TwistWithCovariance = base.geometry_msgs__msg__TwistWithCovariance
geometry_msgs__msg__TwistWithCovarianceStamped = base.geometry_msgs__msg__TwistWithCovarianceStamped
geometry_msgs__msg__Vector3 = base.geometry_msgs__msg__Vector3
geometry_msgs__msg__Vector3Stamped = base.geometry_msgs__msg__Vector3Stamped
geometry_msgs__msg__Wrench = base.geometry_msgs__msg__Wrench
geometry_msgs__msg__WrenchStamped = base.geometry_msgs__msg__WrenchStamped
lifecycle_msgs__msg__State = base.lifecycle_msgs__msg__State
lifecycle_msgs__msg__Transition = base.lifecycle_msgs__msg__Transition
lifecycle_msgs__msg__TransitionDescription = base.lifecycle_msgs__msg__TransitionDescription
lifecycle_msgs__msg__TransitionEvent = base.lifecycle_msgs__msg__TransitionEvent
nav_msgs__msg__GridCells = base.nav_msgs__msg__GridCells
nav_msgs__msg__MapMetaData = base.nav_msgs__msg__MapMetaData
nav_msgs__msg__OccupancyGrid = base.nav_msgs__msg__OccupancyGrid
nav_msgs__msg__Odometry = base.nav_msgs__msg__Odometry
nav_msgs__msg__Path = base.nav_msgs__msg__Path
rcl_interfaces__msg__FloatingPointRange = base.rcl_interfaces__msg__FloatingPointRange
rcl_interfaces__msg__IntegerRange = base.rcl_interfaces__msg__IntegerRange
rcl_interfaces__msg__ListParametersResult = base.rcl_interfaces__msg__ListParametersResult
rcl_interfaces__msg__Log = base.rcl_interfaces__msg__Log
rcl_interfaces__msg__Parameter = base.rcl_interfaces__msg__Parameter
rcl_interfaces__msg__ParameterDescriptor = base.rcl_interfaces__msg__ParameterDescriptor
rcl_interfaces__msg__ParameterEvent = base.rcl_interfaces__msg__ParameterEvent
rcl_interfaces__msg__ParameterEventDescriptors = base.rcl_interfaces__msg__ParameterEventDescriptors
rcl_interfaces__msg__ParameterType = base.rcl_interfaces__msg__ParameterType
rcl_interfaces__msg__ParameterValue = base.rcl_interfaces__msg__ParameterValue
rcl_interfaces__msg__SetParametersResult = base.rcl_interfaces__msg__SetParametersResult
rmw_dds_common__msg__Gid = base.rmw_dds_common__msg__Gid
rmw_dds_common__msg__NodeEntitiesInfo = base.rmw_dds_common__msg__NodeEntitiesInfo
rmw_dds_common__msg__ParticipantEntitiesInfo = base.rmw_dds_common__msg__ParticipantEntitiesInfo
rosgraph_msgs__msg__Clock = base.rosgraph_msgs__msg__Clock
sensor_msgs__msg__BatteryState = base.sensor_msgs__msg__BatteryState
sensor_msgs__msg__CameraInfo = base.sensor_msgs__msg__CameraInfo
sensor_msgs__msg__ChannelFloat32 = base.sensor_msgs__msg__ChannelFloat32
sensor_msgs__msg__CompressedImage = base.sensor_msgs__msg__CompressedImage
sensor_msgs__msg__FluidPressure = base.sensor_msgs__msg__FluidPressure
sensor_msgs__msg__Illuminance = base.sensor_msgs__msg__Illuminance
sensor_msgs__msg__Image = base.sensor_msgs__msg__Image
sensor_msgs__msg__Imu = base.sensor_msgs__msg__Imu
sensor_msgs__msg__JointState = base.sensor_msgs__msg__JointState
sensor_msgs__msg__Joy = base.sensor_msgs__msg__Joy
sensor_msgs__msg__JoyFeedback = base.sensor_msgs__msg__JoyFeedback
sensor_msgs__msg__JoyFeedbackArray = base.sensor_msgs__msg__JoyFeedbackArray
sensor_msgs__msg__LaserEcho = base.sensor_msgs__msg__LaserEcho
sensor_msgs__msg__LaserScan = base.sensor_msgs__msg__LaserScan
sensor_msgs__msg__MagneticField = base.sensor_msgs__msg__MagneticField
sensor_msgs__msg__MultiDOFJointState = base.sensor_msgs__msg__MultiDOFJointState
sensor_msgs__msg__MultiEchoLaserScan = base.sensor_msgs__msg__MultiEchoLaserScan
sensor_msgs__msg__NavSatFix = base.sensor_msgs__msg__NavSatFix
sensor_msgs__msg__NavSatStatus = base.sensor_msgs__msg__NavSatStatus
sensor_msgs__msg__PointCloud = base.sensor_msgs__msg__PointCloud
sensor_msgs__msg__PointCloud2 = base.sensor_msgs__msg__PointCloud2
sensor_msgs__msg__PointField = base.sensor_msgs__msg__PointField
sensor_msgs__msg__Range = base.sensor_msgs__msg__Range
sensor_msgs__msg__RegionOfInterest = base.sensor_msgs__msg__RegionOfInterest
sensor_msgs__msg__RelativeHumidity = base.sensor_msgs__msg__RelativeHumidity
sensor_msgs__msg__Temperature = base.sensor_msgs__msg__Temperature
sensor_msgs__msg__TimeReference = base.sensor_msgs__msg__TimeReference
shape_msgs__msg__Mesh = base.shape_msgs__msg__Mesh
shape_msgs__msg__MeshTriangle = base.shape_msgs__msg__MeshTriangle
shape_msgs__msg__Plane = base.shape_msgs__msg__Plane
statistics_msgs__msg__MetricsMessage = base.statistics_msgs__msg__MetricsMessage
statistics_msgs__msg__StatisticDataPoint = base.statistics_msgs__msg__StatisticDataPoint
statistics_msgs__msg__StatisticDataType = base.statistics_msgs__msg__StatisticDataType
std_msgs__msg__Bool = base.std_msgs__msg__Bool
std_msgs__msg__Byte = base.std_msgs__msg__Byte
std_msgs__msg__ByteMultiArray = base.std_msgs__msg__ByteMultiArray
std_msgs__msg__Char = base.std_msgs__msg__Char
std_msgs__msg__ColorRGBA = base.std_msgs__msg__ColorRGBA
std_msgs__msg__Empty = base.std_msgs__msg__Empty
std_msgs__msg__Float32 = base.std_msgs__msg__Float32
std_msgs__msg__Float32MultiArray = base.std_msgs__msg__Float32MultiArray
std_msgs__msg__Float64 = base.std_msgs__msg__Float64
std_msgs__msg__Float64MultiArray = base.std_msgs__msg__Float64MultiArray
std_msgs__msg__Header = base.std_msgs__msg__Header
std_msgs__msg__Int16 = base.std_msgs__msg__Int16
std_msgs__msg__Int16MultiArray = base.std_msgs__msg__Int16MultiArray
std_msgs__msg__Int32 = base.std_msgs__msg__Int32
std_msgs__msg__Int32MultiArray = base.std_msgs__msg__Int32MultiArray
std_msgs__msg__Int64 = base.std_msgs__msg__Int64
std_msgs__msg__Int64MultiArray = base.std_msgs__msg__Int64MultiArray
std_msgs__msg__Int8 = base.std_msgs__msg__Int8
std_msgs__msg__Int8MultiArray = base.std_msgs__msg__Int8MultiArray
std_msgs__msg__MultiArrayDimension = base.std_msgs__msg__MultiArrayDimension
std_msgs__msg__MultiArrayLayout = base.std_msgs__msg__MultiArrayLayout
std_msgs__msg__String = base.std_msgs__msg__String
std_msgs__msg__UInt16 = base.std_msgs__msg__UInt16
std_msgs__msg__UInt16MultiArray = base.std_msgs__msg__UInt16MultiArray
std_msgs__msg__UInt32 = base.std_msgs__msg__UInt32
std_msgs__msg__UInt32MultiArray = base.std_msgs__msg__UInt32MultiArray
std_msgs__msg__UInt64 = base.std_msgs__msg__UInt64
std_msgs__msg__UInt64MultiArray = base.std_msgs__msg__UInt64MultiArray
std_msgs__msg__UInt8 = base.std_msgs__msg__UInt8
std_msgs__msg__UInt8MultiArray = base.std_msgs__msg__UInt8MultiArray
stereo_msgs__msg__DisparityImage = base.stereo_msgs__msg__DisparityImage
tf2_msgs__msg__TF2Error = base.tf2_msgs__msg__TF2Error
tf2_msgs__msg__TFMessage = base.tf2_msgs__msg__TFMessage
trajectory_msgs__msg__JointTrajectory = base.trajectory_msgs__msg__JointTrajectory
trajectory_msgs__msg__JointTrajectoryPoint = base.trajectory_msgs__msg__JointTrajectoryPoint
trajectory_msgs__msg__MultiDOFJointTrajectory = base.trajectory_msgs__msg__MultiDOFJointTrajectory
trajectory_msgs__msg__MultiDOFJointTrajectoryPoint = (
    base.trajectory_msgs__msg__MultiDOFJointTrajectoryPoint
)
unique_identifier_msgs__msg__UUID = base.unique_identifier_msgs__msg__UUID
visualization_msgs__msg__ImageMarker = base.visualization_msgs__msg__ImageMarker
visualization_msgs__msg__InteractiveMarker = base.visualization_msgs__msg__InteractiveMarker
visualization_msgs__msg__InteractiveMarkerControl = (
    base.visualization_msgs__msg__InteractiveMarkerControl
)
visualization_msgs__msg__InteractiveMarkerFeedback = (
    base.visualization_msgs__msg__InteractiveMarkerFeedback
)
visualization_msgs__msg__InteractiveMarkerInit = base.visualization_msgs__msg__InteractiveMarkerInit
visualization_msgs__msg__InteractiveMarkerPose = base.visualization_msgs__msg__InteractiveMarkerPose
visualization_msgs__msg__InteractiveMarkerUpdate = (
    base.visualization_msgs__msg__InteractiveMarkerUpdate
)
visualization_msgs__msg__MarkerArray = base.visualization_msgs__msg__MarkerArray
visualization_msgs__msg__MenuEntry = base.visualization_msgs__msg__MenuEntry


@dataclass
class rosbag2_interfaces__msg__ReadSplitEvent:
    """Class for rosbag2_interfaces/msg/ReadSplitEvent."""

    closed_file: str
    opened_file: str
    __msgtype__: ClassVar[str] = 'rosbag2_interfaces/msg/ReadSplitEvent'


@dataclass
class rosbag2_interfaces__msg__WriteSplitEvent:
    """Class for rosbag2_interfaces/msg/WriteSplitEvent."""

    closed_file: str
    opened_file: str
    __msgtype__: ClassVar[str] = 'rosbag2_interfaces/msg/WriteSplitEvent'


@dataclass
class shape_msgs__msg__SolidPrimitive:
    """Class for shape_msgs/msg/SolidPrimitive."""

    type: int
    dimensions: np.ndarray[None, np.dtype[np.float64]]
    polygon: geometry_msgs__msg__Polygon
    BOX: ClassVar[int] = 1
    SPHERE: ClassVar[int] = 2
    CYLINDER: ClassVar[int] = 3
    CONE: ClassVar[int] = 4
    PRISM: ClassVar[int] = 5
    BOX_X: ClassVar[int] = 0
    BOX_Y: ClassVar[int] = 1
    BOX_Z: ClassVar[int] = 2
    SPHERE_RADIUS: ClassVar[int] = 0
    CYLINDER_HEIGHT: ClassVar[int] = 0
    CYLINDER_RADIUS: ClassVar[int] = 1
    CONE_HEIGHT: ClassVar[int] = 0
    CONE_RADIUS: ClassVar[int] = 1
    PRISM_HEIGHT: ClassVar[int] = 0
    __msgtype__: ClassVar[str] = 'shape_msgs/msg/SolidPrimitive'


@dataclass
class visualization_msgs__msg__Marker:
    """Class for visualization_msgs/msg/Marker."""

    header: std_msgs__msg__Header
    ns: str
    id: int
    type: int
    action: int
    pose: geometry_msgs__msg__Pose
    scale: geometry_msgs__msg__Vector3
    color: std_msgs__msg__ColorRGBA
    lifetime: builtin_interfaces__msg__Duration
    frame_locked: bool
    points: list[geometry_msgs__msg__Point]
    colors: list[std_msgs__msg__ColorRGBA]
    texture_resource: str
    texture: sensor_msgs__msg__CompressedImage
    uv_coordinates: list[visualization_msgs__msg__UVCoordinate]
    text: str
    mesh_resource: str
    mesh_file: visualization_msgs__msg__MeshFile
    mesh_use_embedded_materials: bool
    ARROW: ClassVar[int] = 0
    CUBE: ClassVar[int] = 1
    SPHERE: ClassVar[int] = 2
    CYLINDER: ClassVar[int] = 3
    LINE_STRIP: ClassVar[int] = 4
    LINE_LIST: ClassVar[int] = 5
    CUBE_LIST: ClassVar[int] = 6
    SPHERE_LIST: ClassVar[int] = 7
    POINTS: ClassVar[int] = 8
    TEXT_VIEW_FACING: ClassVar[int] = 9
    MESH_RESOURCE: ClassVar[int] = 10
    TRIANGLE_LIST: ClassVar[int] = 11
    ADD: ClassVar[int] = 0
    MODIFY: ClassVar[int] = 0
    DELETE: ClassVar[int] = 2
    DELETEALL: ClassVar[int] = 3
    __msgtype__: ClassVar[str] = 'visualization_msgs/msg/Marker'


@dataclass
class visualization_msgs__msg__MeshFile:
    """Class for visualization_msgs/msg/MeshFile."""

    filename: str
    data: np.ndarray[None, np.dtype[np.uint8]]
    __msgtype__: ClassVar[str] = 'visualization_msgs/msg/MeshFile'


@dataclass
class visualization_msgs__msg__UVCoordinate:
    """Class for visualization_msgs/msg/UVCoordinate."""

    u: float
    v: float
    __msgtype__: ClassVar[str] = 'visualization_msgs/msg/UVCoordinate'


FIELDDEFS: Typesdict = {
    'action_msgs/msg/GoalInfo': base.FIELDDEFS['action_msgs/msg/GoalInfo'],
    'action_msgs/msg/GoalStatus': base.FIELDDEFS['action_msgs/msg/GoalStatus'],
    'action_msgs/msg/GoalStatusArray': base.FIELDDEFS['action_msgs/msg/GoalStatusArray'],
    'actionlib_msgs/msg/GoalID': base.FIELDDEFS['actionlib_msgs/msg/GoalID'],
    'actionlib_msgs/msg/GoalStatus': base.FIELDDEFS['actionlib_msgs/msg/GoalStatus'],
    'actionlib_msgs/msg/GoalStatusArray': base.FIELDDEFS['actionlib_msgs/msg/GoalStatusArray'],
    'builtin_interfaces/msg/Duration': base.FIELDDEFS['builtin_interfaces/msg/Duration'],
    'builtin_interfaces/msg/Time': base.FIELDDEFS['builtin_interfaces/msg/Time'],
    'diagnostic_msgs/msg/DiagnosticArray': base.FIELDDEFS['diagnostic_msgs/msg/DiagnosticArray'],
    'diagnostic_msgs/msg/DiagnosticStatus': base.FIELDDEFS['diagnostic_msgs/msg/DiagnosticStatus'],
    'diagnostic_msgs/msg/KeyValue': base.FIELDDEFS['diagnostic_msgs/msg/KeyValue'],
    'geometry_msgs/msg/Accel': base.FIELDDEFS['geometry_msgs/msg/Accel'],
    'geometry_msgs/msg/AccelStamped': base.FIELDDEFS['geometry_msgs/msg/AccelStamped'],
    'geometry_msgs/msg/AccelWithCovariance': base.FIELDDEFS[
        'geometry_msgs/msg/AccelWithCovariance'
    ],
    'geometry_msgs/msg/AccelWithCovarianceStamped': base.FIELDDEFS[
        'geometry_msgs/msg/AccelWithCovarianceStamped'
    ],
    'geometry_msgs/msg/Inertia': base.FIELDDEFS['geometry_msgs/msg/Inertia'],
    'geometry_msgs/msg/InertiaStamped': base.FIELDDEFS['geometry_msgs/msg/InertiaStamped'],
    'geometry_msgs/msg/Point': base.FIELDDEFS['geometry_msgs/msg/Point'],
    'geometry_msgs/msg/Point32': base.FIELDDEFS['geometry_msgs/msg/Point32'],
    'geometry_msgs/msg/PointStamped': base.FIELDDEFS['geometry_msgs/msg/PointStamped'],
    'geometry_msgs/msg/Polygon': base.FIELDDEFS['geometry_msgs/msg/Polygon'],
    'geometry_msgs/msg/PolygonStamped': base.FIELDDEFS['geometry_msgs/msg/PolygonStamped'],
    'geometry_msgs/msg/Pose': base.FIELDDEFS['geometry_msgs/msg/Pose'],
    'geometry_msgs/msg/Pose2D': base.FIELDDEFS['geometry_msgs/msg/Pose2D'],
    'geometry_msgs/msg/PoseArray': base.FIELDDEFS['geometry_msgs/msg/PoseArray'],
    'geometry_msgs/msg/PoseStamped': base.FIELDDEFS['geometry_msgs/msg/PoseStamped'],
    'geometry_msgs/msg/PoseWithCovariance': base.FIELDDEFS['geometry_msgs/msg/PoseWithCovariance'],
    'geometry_msgs/msg/PoseWithCovarianceStamped': base.FIELDDEFS[
        'geometry_msgs/msg/PoseWithCovarianceStamped'
    ],
    'geometry_msgs/msg/Quaternion': base.FIELDDEFS['geometry_msgs/msg/Quaternion'],
    'geometry_msgs/msg/QuaternionStamped': base.FIELDDEFS['geometry_msgs/msg/QuaternionStamped'],
    'geometry_msgs/msg/Transform': base.FIELDDEFS['geometry_msgs/msg/Transform'],
    'geometry_msgs/msg/TransformStamped': base.FIELDDEFS['geometry_msgs/msg/TransformStamped'],
    'geometry_msgs/msg/Twist': base.FIELDDEFS['geometry_msgs/msg/Twist'],
    'geometry_msgs/msg/TwistStamped': base.FIELDDEFS['geometry_msgs/msg/TwistStamped'],
    'geometry_msgs/msg/TwistWithCovariance': base.FIELDDEFS[
        'geometry_msgs/msg/TwistWithCovariance'
    ],
    'geometry_msgs/msg/TwistWithCovarianceStamped': base.FIELDDEFS[
        'geometry_msgs/msg/TwistWithCovarianceStamped'
    ],
    'geometry_msgs/msg/Vector3': base.FIELDDEFS['geometry_msgs/msg/Vector3'],
    'geometry_msgs/msg/Vector3Stamped': base.FIELDDEFS['geometry_msgs/msg/Vector3Stamped'],
    'geometry_msgs/msg/Wrench': base.FIELDDEFS['geometry_msgs/msg/Wrench'],
    'geometry_msgs/msg/WrenchStamped': base.FIELDDEFS['geometry_msgs/msg/WrenchStamped'],
    'lifecycle_msgs/msg/State': base.FIELDDEFS['lifecycle_msgs/msg/State'],
    'lifecycle_msgs/msg/Transition': base.FIELDDEFS['lifecycle_msgs/msg/Transition'],
    'lifecycle_msgs/msg/TransitionDescription': base.FIELDDEFS[
        'lifecycle_msgs/msg/TransitionDescription'
    ],
    'lifecycle_msgs/msg/TransitionEvent': base.FIELDDEFS['lifecycle_msgs/msg/TransitionEvent'],
    'nav_msgs/msg/GridCells': base.FIELDDEFS['nav_msgs/msg/GridCells'],
    'nav_msgs/msg/MapMetaData': base.FIELDDEFS['nav_msgs/msg/MapMetaData'],
    'nav_msgs/msg/OccupancyGrid': base.FIELDDEFS['nav_msgs/msg/OccupancyGrid'],
    'nav_msgs/msg/Odometry': base.FIELDDEFS['nav_msgs/msg/Odometry'],
    'nav_msgs/msg/Path': base.FIELDDEFS['nav_msgs/msg/Path'],
    'rcl_interfaces/msg/FloatingPointRange': base.FIELDDEFS[
        'rcl_interfaces/msg/FloatingPointRange'
    ],
    'rcl_interfaces/msg/IntegerRange': base.FIELDDEFS['rcl_interfaces/msg/IntegerRange'],
    'rcl_interfaces/msg/ListParametersResult': base.FIELDDEFS[
        'rcl_interfaces/msg/ListParametersResult'
    ],
    'rcl_interfaces/msg/Log': base.FIELDDEFS['rcl_interfaces/msg/Log'],
    'rcl_interfaces/msg/Parameter': base.FIELDDEFS['rcl_interfaces/msg/Parameter'],
    'rcl_interfaces/msg/ParameterDescriptor': base.FIELDDEFS[
        'rcl_interfaces/msg/ParameterDescriptor'
    ],
    'rcl_interfaces/msg/ParameterEvent': base.FIELDDEFS['rcl_interfaces/msg/ParameterEvent'],
    'rcl_interfaces/msg/ParameterEventDescriptors': base.FIELDDEFS[
        'rcl_interfaces/msg/ParameterEventDescriptors'
    ],
    'rcl_interfaces/msg/ParameterType': base.FIELDDEFS['rcl_interfaces/msg/ParameterType'],
    'rcl_interfaces/msg/ParameterValue': base.FIELDDEFS['rcl_interfaces/msg/ParameterValue'],
    'rcl_interfaces/msg/SetParametersResult': base.FIELDDEFS[
        'rcl_interfaces/msg/SetParametersResult'
    ],
    'rmw_dds_common/msg/Gid': base.FIELDDEFS['rmw_dds_common/msg/Gid'],
    'rmw_dds_common/msg/NodeEntitiesInfo': base.FIELDDEFS['rmw_dds_common/msg/NodeEntitiesInfo'],
    'rmw_dds_common/msg/ParticipantEntitiesInfo': base.FIELDDEFS[
        'rmw_dds_common/msg/ParticipantEntitiesInfo'
    ],
    'rosgraph_msgs/msg/Clock': base.FIELDDEFS['rosgraph_msgs/msg/Clock'],
    'sensor_msgs/msg/BatteryState': base.FIELDDEFS['sensor_msgs/msg/BatteryState'],
    'sensor_msgs/msg/CameraInfo': base.FIELDDEFS['sensor_msgs/msg/CameraInfo'],
    'sensor_msgs/msg/ChannelFloat32': base.FIELDDEFS['sensor_msgs/msg/ChannelFloat32'],
    'sensor_msgs/msg/CompressedImage': base.FIELDDEFS['sensor_msgs/msg/CompressedImage'],
    'sensor_msgs/msg/FluidPressure': base.FIELDDEFS['sensor_msgs/msg/FluidPressure'],
    'sensor_msgs/msg/Illuminance': base.FIELDDEFS['sensor_msgs/msg/Illuminance'],
    'sensor_msgs/msg/Image': base.FIELDDEFS['sensor_msgs/msg/Image'],
    'sensor_msgs/msg/Imu': base.FIELDDEFS['sensor_msgs/msg/Imu'],
    'sensor_msgs/msg/JointState': base.FIELDDEFS['sensor_msgs/msg/JointState'],
    'sensor_msgs/msg/Joy': base.FIELDDEFS['sensor_msgs/msg/Joy'],
    'sensor_msgs/msg/JoyFeedback': base.FIELDDEFS['sensor_msgs/msg/JoyFeedback'],
    'sensor_msgs/msg/JoyFeedbackArray': base.FIELDDEFS['sensor_msgs/msg/JoyFeedbackArray'],
    'sensor_msgs/msg/LaserEcho': base.FIELDDEFS['sensor_msgs/msg/LaserEcho'],
    'sensor_msgs/msg/LaserScan': base.FIELDDEFS['sensor_msgs/msg/LaserScan'],
    'sensor_msgs/msg/MagneticField': base.FIELDDEFS['sensor_msgs/msg/MagneticField'],
    'sensor_msgs/msg/MultiDOFJointState': base.FIELDDEFS['sensor_msgs/msg/MultiDOFJointState'],
    'sensor_msgs/msg/MultiEchoLaserScan': base.FIELDDEFS['sensor_msgs/msg/MultiEchoLaserScan'],
    'sensor_msgs/msg/NavSatFix': base.FIELDDEFS['sensor_msgs/msg/NavSatFix'],
    'sensor_msgs/msg/NavSatStatus': base.FIELDDEFS['sensor_msgs/msg/NavSatStatus'],
    'sensor_msgs/msg/PointCloud': base.FIELDDEFS['sensor_msgs/msg/PointCloud'],
    'sensor_msgs/msg/PointCloud2': base.FIELDDEFS['sensor_msgs/msg/PointCloud2'],
    'sensor_msgs/msg/PointField': base.FIELDDEFS['sensor_msgs/msg/PointField'],
    'sensor_msgs/msg/Range': base.FIELDDEFS['sensor_msgs/msg/Range'],
    'sensor_msgs/msg/RegionOfInterest': base.FIELDDEFS['sensor_msgs/msg/RegionOfInterest'],
    'sensor_msgs/msg/RelativeHumidity': base.FIELDDEFS['sensor_msgs/msg/RelativeHumidity'],
    'sensor_msgs/msg/Temperature': base.FIELDDEFS['sensor_msgs/msg/Temperature'],
    'sensor_msgs/msg/TimeReference': base.FIELDDEFS['sensor_msgs/msg/TimeReference'],
    'shape_msgs/msg/Mesh': base.FIELDDEFS['shape_msgs/msg/Mesh'],
    'shape_msgs/msg/MeshTriangle': base.FIELDDEFS['shape_msgs/msg/MeshTriangle'],
    'shape_msgs/msg/Plane': base.FIELDDEFS['shape_msgs/msg/Plane'],
    'statistics_msgs/msg/MetricsMessage': base.FIELDDEFS['statistics_msgs/msg/MetricsMessage'],
    'statistics_msgs/msg/StatisticDataPoint': base.FIELDDEFS[
        'statistics_msgs/msg/StatisticDataPoint'
    ],
    'statistics_msgs/msg/StatisticDataType': base.FIELDDEFS[
        'statistics_msgs/msg/StatisticDataType'
    ],
    'std_msgs/msg/Bool': base.FIELDDEFS['std_msgs/msg/Bool'],
    'std_msgs/msg/Byte': base.FIELDDEFS['std_msgs/msg/Byte'],
    'std_msgs/msg/ByteMultiArray': base.FIELDDEFS['std_msgs/msg/ByteMultiArray'],
    'std_msgs/msg/Char': base.FIELDDEFS['std_msgs/msg/Char'],
    'std_msgs/msg/ColorRGBA': base.FIELDDEFS['std_msgs/msg/ColorRGBA'],
    'std_msgs/msg/Empty': base.FIELDDEFS['std_msgs/msg/Empty'],
    'std_msgs/msg/Float32': base.FIELDDEFS['std_msgs/msg/Float32'],
    'std_msgs/msg/Float32MultiArray': base.FIELDDEFS['std_msgs/msg/Float32MultiArray'],
    'std_msgs/msg/Float64': base.FIELDDEFS['std_msgs/msg/Float64'],
    'std_msgs/msg/Float64MultiArray': base.FIELDDEFS['std_msgs/msg/Float64MultiArray'],
    'std_msgs/msg/Header': base.FIELDDEFS['std_msgs/msg/Header'],
    'std_msgs/msg/Int16': base.FIELDDEFS['std_msgs/msg/Int16'],
    'std_msgs/msg/Int16MultiArray': base.FIELDDEFS['std_msgs/msg/Int16MultiArray'],
    'std_msgs/msg/Int32': base.FIELDDEFS['std_msgs/msg/Int32'],
    'std_msgs/msg/Int32MultiArray': base.FIELDDEFS['std_msgs/msg/Int32MultiArray'],
    'std_msgs/msg/Int64': base.FIELDDEFS['std_msgs/msg/Int64'],
    'std_msgs/msg/Int64MultiArray': base.FIELDDEFS['std_msgs/msg/Int64MultiArray'],
    'std_msgs/msg/Int8': base.FIELDDEFS['std_msgs/msg/Int8'],
    'std_msgs/msg/Int8MultiArray': base.FIELDDEFS['std_msgs/msg/Int8MultiArray'],
    'std_msgs/msg/MultiArrayDimension': base.FIELDDEFS['std_msgs/msg/MultiArrayDimension'],
    'std_msgs/msg/MultiArrayLayout': base.FIELDDEFS['std_msgs/msg/MultiArrayLayout'],
    'std_msgs/msg/String': base.FIELDDEFS['std_msgs/msg/String'],
    'std_msgs/msg/UInt16': base.FIELDDEFS['std_msgs/msg/UInt16'],
    'std_msgs/msg/UInt16MultiArray': base.FIELDDEFS['std_msgs/msg/UInt16MultiArray'],
    'std_msgs/msg/UInt32': base.FIELDDEFS['std_msgs/msg/UInt32'],
    'std_msgs/msg/UInt32MultiArray': base.FIELDDEFS['std_msgs/msg/UInt32MultiArray'],
    'std_msgs/msg/UInt64': base.FIELDDEFS['std_msgs/msg/UInt64'],
    'std_msgs/msg/UInt64MultiArray': base.FIELDDEFS['std_msgs/msg/UInt64MultiArray'],
    'std_msgs/msg/UInt8': base.FIELDDEFS['std_msgs/msg/UInt8'],
    'std_msgs/msg/UInt8MultiArray': base.FIELDDEFS['std_msgs/msg/UInt8MultiArray'],
    'stereo_msgs/msg/DisparityImage': base.FIELDDEFS['stereo_msgs/msg/DisparityImage'],
    'tf2_msgs/msg/TF2Error': base.FIELDDEFS['tf2_msgs/msg/TF2Error'],
    'tf2_msgs/msg/TFMessage': base.FIELDDEFS['tf2_msgs/msg/TFMessage'],
    'trajectory_msgs/msg/JointTrajectory': base.FIELDDEFS['trajectory_msgs/msg/JointTrajectory'],
    'trajectory_msgs/msg/JointTrajectoryPoint': base.FIELDDEFS[
        'trajectory_msgs/msg/JointTrajectoryPoint'
    ],
    'trajectory_msgs/msg/MultiDOFJointTrajectory': base.FIELDDEFS[
        'trajectory_msgs/msg/MultiDOFJointTrajectory'
    ],
    'trajectory_msgs/msg/MultiDOFJointTrajectoryPoint': base.FIELDDEFS[
        'trajectory_msgs/msg/MultiDOFJointTrajectoryPoint'
    ],
    'unique_identifier_msgs/msg/UUID': base.FIELDDEFS['unique_identifier_msgs/msg/UUID'],
    'visualization_msgs/msg/ImageMarker': base.FIELDDEFS['visualization_msgs/msg/ImageMarker'],
    'visualization_msgs/msg/InteractiveMarker': base.FIELDDEFS[
        'visualization_msgs/msg/InteractiveMarker'
    ],
    'visualization_msgs/msg/InteractiveMarkerControl': base.FIELDDEFS[
        'visualization_msgs/msg/InteractiveMarkerControl'
    ],
    'visualization_msgs/msg/InteractiveMarkerFeedback': base.FIELDDEFS[
        'visualization_msgs/msg/InteractiveMarkerFeedback'
    ],
    'visualization_msgs/msg/InteractiveMarkerInit': base.FIELDDEFS[
        'visualization_msgs/msg/InteractiveMarkerInit'
    ],
    'visualization_msgs/msg/InteractiveMarkerPose': base.FIELDDEFS[
        'visualization_msgs/msg/InteractiveMarkerPose'
    ],
    'visualization_msgs/msg/InteractiveMarkerUpdate': base.FIELDDEFS[
        'visualization_msgs/msg/InteractiveMarkerUpdate'
    ],
    'visualization_msgs/msg/MarkerArray': base.FIELDDEFS['visualization_msgs/msg/MarkerArray'],
    'visualization_msgs/msg/MenuEntry': base.FIELDDEFS['visualization_msgs/msg/MenuEntry'],
    'rosbag2_interfaces/msg/ReadSplitEvent': (
        [],
        [
            ('closed_file', (T.BASE, ('string', 0))),
            ('opened_file', (T.BASE, ('string', 0))),
        ],
    ),
    'rosbag2_interfaces/msg/WriteSplitEvent': (
        [],
        [
            ('closed_file', (T.BASE, ('string', 0))),
            ('opened_file', (T.BASE, ('string', 0))),
        ],
    ),
    'shape_msgs/msg/SolidPrimitive': (
        [
            ('BOX', 'uint8', 1),
            ('SPHERE', 'uint8', 2),
            ('CYLINDER', 'uint8', 3),
            ('CONE', 'uint8', 4),
            ('PRISM', 'uint8', 5),
            ('BOX_X', 'uint8', 0),
            ('BOX_Y', 'uint8', 1),
            ('BOX_Z', 'uint8', 2),
            ('SPHERE_RADIUS', 'uint8', 0),
            ('CYLINDER_HEIGHT', 'uint8', 0),
            ('CYLINDER_RADIUS', 'uint8', 1),
            ('CONE_HEIGHT', 'uint8', 0),
            ('CONE_RADIUS', 'uint8', 1),
            ('PRISM_HEIGHT', 'uint8', 0),
        ],
        [
            ('type', (T.BASE, ('uint8', 0))),
            ('dimensions', (T.SEQUENCE, ((T.BASE, ('float64', 0)), 3))),
            ('polygon', (T.NAME, 'geometry_msgs/msg/Polygon')),
        ],
    ),
    'visualization_msgs/msg/Marker': (
        [
            ('ARROW', 'int32', 0),
            ('CUBE', 'int32', 1),
            ('SPHERE', 'int32', 2),
            ('CYLINDER', 'int32', 3),
            ('LINE_STRIP', 'int32', 4),
            ('LINE_LIST', 'int32', 5),
            ('CUBE_LIST', 'int32', 6),
            ('SPHERE_LIST', 'int32', 7),
            ('POINTS', 'int32', 8),
            ('TEXT_VIEW_FACING', 'int32', 9),
            ('MESH_RESOURCE', 'int32', 10),
            ('TRIANGLE_LIST', 'int32', 11),
            ('ADD', 'int32', 0),
            ('MODIFY', 'int32', 0),
            ('DELETE', 'int32', 2),
            ('DELETEALL', 'int32', 3),
        ],
        [
            ('header', (T.NAME, 'std_msgs/msg/Header')),
            ('ns', (T.BASE, ('string', 0))),
            ('id', (T.BASE, ('int32', 0))),
            ('type', (T.BASE, ('int32', 0))),
            ('action', (T.BASE, ('int32', 0))),
            ('pose', (T.NAME, 'geometry_msgs/msg/Pose')),
            ('scale', (T.NAME, 'geometry_msgs/msg/Vector3')),
            ('color', (T.NAME, 'std_msgs/msg/ColorRGBA')),
            ('lifetime', (T.NAME, 'builtin_interfaces/msg/Duration')),
            ('frame_locked', (T.BASE, ('bool', 0))),
            ('points', (T.SEQUENCE, ((T.NAME, 'geometry_msgs/msg/Point'), 0))),
            ('colors', (T.SEQUENCE, ((T.NAME, 'std_msgs/msg/ColorRGBA'), 0))),
            ('texture_resource', (T.BASE, ('string', 0))),
            ('texture', (T.NAME, 'sensor_msgs/msg/CompressedImage')),
            ('uv_coordinates', (T.SEQUENCE, ((T.NAME, 'visualization_msgs/msg/UVCoordinate'), 0))),
            ('text', (T.BASE, ('string', 0))),
            ('mesh_resource', (T.BASE, ('string', 0))),
            ('mesh_file', (T.NAME, 'visualization_msgs/msg/MeshFile')),
            ('mesh_use_embedded_materials', (T.BASE, ('bool', 0))),
        ],
    ),
    'visualization_msgs/msg/MeshFile': (
        [],
        [
            ('filename', (T.BASE, ('string', 0))),
            ('data', (T.SEQUENCE, ((T.BASE, ('uint8', 0)), 0))),
        ],
    ),
    'visualization_msgs/msg/UVCoordinate': (
        [],
        [
            ('u', (T.BASE, ('float32', 0))),
            ('v', (T.BASE, ('float32', 0))),
        ],
    ),
}
