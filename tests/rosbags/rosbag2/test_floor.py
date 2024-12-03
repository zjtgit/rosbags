from pathlib import Path

from rosbags.highlevel import AnyReader
from rosbags.typesys import Stores, get_typestore, get_types_from_msg, get_types_from_idl
from rosbags.convert.converter import generate_message_converter, migrate_message

from rclpy.clock import Clock
from rclpy.duration import Duration
from rclpy.serialization import serialize_message
from example_interfaces.msg import Int32
from sensor_msgs.msg import Imu, LaserScan
# from nav_msgs.msg import
from sad_msgs.msg import *
from sad_dataset_msgs.msg import *
from builtin_interfaces.msg import *
from rosbags.rosbag2 import Writer
import rosbag2_py
import sys
import os
import shutil
def header_ros1_to_ros2(ros1, ros2):
    ros2.frame_id = ros1.frame_id
    ros2.stamp.sec = ros1.stamp.sec
    ros2.stamp.nanosec = ros1.stamp.nanosec

def cov_pos_ros1_to_ros2(ros1, ros2):
    ros2.covariance = ros1.covariance
    pose_ros1_to_ros2(ros1.pose, ros2.pose)
def pose_ros1_to_ros2(ros1, ros2):
    quat_ros1_to_ros2(ros1.orientation, ros2.orientation)
    pos_ros1_to_ros2(ros1.position, ros2.position)
def quat_ros1_to_ros2(ros1, ros2):
    # local_data.fusion_pose.covariance = msg.fusion_pose.covariance
    ros2.x = ros1.x
    ros2.y = ros1.y
    ros2.z = ros1.z
    ros2.w = ros1.w
def pos_ros1_to_ros2(ros1, ros2):
    ros2.x = ros1.x
    ros2.y = ros1.y
    ros2.z = ros1.z


def guess_msgtype(path: Path) -> str:
    """Guess message type name from path."""
    name = path.relative_to(path.parents[2]).with_suffix('')
    if 'msg' not in name.parts:
        name = name.parent / 'msg' / name.name
    return str(name)

# bagpath = Path('/media/zjt/T7/sad/2dmapping/floor1.bag')
# bagpath = Path('/media/zjt/T7/sad/2dmapping/floor2.bag')
# bagpath = Path('/media/zjt/T7/sad/2dmapping/floor3.bag')
bagpath = Path('/media/zjt/T7/sad/2dmapping/floor4.bag')

imu_data = Ivsensorimu()
local_data = Localization()
can_data = CanSensor()
laser_data = LaserScan()

# Create a type store to use if the bag has no message definitions.
# typestore = get_typestore(Stores.ROS2_HUMBLE)
typestore = get_typestore(Stores.ROS1_NOETIC)

srcts = get_typestore(Stores.ROS1_NOETIC)
dstts = get_typestore(Stores.ROS2_HUMBLE)

typestore = get_typestore(Stores.ROS2_FOXY)
# typestore.register()
add_types = {}
paths = ['/home/zjt/code/slam_ros2/src/sad_dataset_msgs/msg/Ivsensorimu.msg',
        '/home/zjt/code/slam_ros2/src/sad_dataset_msgs/msg/Localization.msg',
        '/home/zjt/code/slam_ros2/src/sad_dataset_msgs/msg/CanSensor.msg',
        '/home/zjt/code/slam_ros2/src/sad_dataset_msgs/msg/Ultrasonicradar.msg',
        '/home/zjt/code/slam_ros2/src/sad_dataset_msgs/msg/WheelSpeed.msg',
        '/home/zjt/code/slam_ros2/src/sad_msgs/msg/VelodyneScan.msg',
        '/home/zjt/code/slam_ros2/src/sad_msgs/msg/VelodynePacket.msg',
        '/home/zjt/code/slam_ros2/src/sad_msgs/msg/VelodyneScanRaw.msg',
        '/home/zjt/code/slam_ros2/src/sad_msgs/msg/FaultVec.msg',
        '/home/zjt/code/slam_ros2/src/sad_msgs/msg/FaultInfo.msg']

for p in paths:
    msgpath = Path(p)
    msgdef = msgpath.read_text(encoding='utf-8')
    add_types.update(get_types_from_msg(msgdef, guess_msgtype(msgpath)))

typestore.register(add_types)


# Ivsensorimu = typestore.types['sad_dataset_msgs/msg/Ivsensorimu']
# Localization = typestore.types['sad_dataset_msgs/msg/Localization']
# CanSensor = typestore.types['sad_dataset_msgs/msg/CanSensor']



# add_types.update(get_types_from_idl(imu_data, 'sad_dataset_msgs/msg/Ivsensorimu'))
# add_types.update(get_types_from_msg(local_data, 'sad_dataset_msgs/msg/Localization'))
# add_types.update(get_types_from_msg(can_data, 'sad_dataset_msgs/msg/CanSensor'))

# typestore.register(add_types)
# add_types.update(get_types_from_msg(imu_data, 'sad_dataset_msgs/msg/Ivsensorimu'))


# ros2 writer
# path = Path('/home/zjt/code/slam_ros2/src/sad/data/rosbag2/floor1')
# path = Path('/home/zjt/code/slam_ros2/src/sad/data/rosbag2/floor2')
# path = Path('/home/zjt/code/slam_ros2/src/sad/data/rosbag2/floor3')
path = Path('/home/zjt/code/slam_ros2/src/sad/data/rosbag2/floor4')

if path.exists():
    print(path.absolute())
    shutil.rmtree(path.absolute())

# writer = Writer(path)


# writer = rosbag2_py.SequentialWriter()
# storage_options = rosbag2_py._storage.StorageOptions(
#     uri='/home/zjt/code/slam_ros2/src/sad/data/rosbag2/floor1',
#     storage_id='sqlite3')
# converter_options = rosbag2_py._storage.ConverterOptions('', '')
# writer.open(storage_options, converter_options)
#
# imu_topic_info = rosbag2_py._storage.TopicMetadata(
#     name='floor1/Imu',
#     type='sad_dataset_msgs/msg/Ivsensorimu',
#     serialization_format='cdr')
# writer.create_topic(imu_topic_info)
#
# local_topic_info = rosbag2_py._storage.TopicMetadata(
#     name='floor1/Localization',
#     type='sad_dataset_msgs/msg/Localization',
#     serialization_format='cdr')
# writer.create_topic(local_topic_info)
#
#
# can_topic_info = rosbag2_py._storage.TopicMetadata(
#     name='floor1/CanSensor',
#     type='sad_dataset_msgs/msg/CanSensor',
#     serialization_format='cdr')
# writer.create_topic(can_topic_info)

# laser_topic_info = rosbag2_py._storage.TopicMetadata(
#     name='floor1/LaserScan',
#     type='sensor_msgs/msg/LaserScan',
#     serialization_format='cdr')
# writer.create_topic(laser_topic_info)


# imu_data = Ivsensorimu()
# local_data = Localization()
# can_data = CanSensor()
# laser_data = LaserScan()


# Create reader instance and open for reading.
with AnyReader([bagpath], default_typestore=typestore) as reader, Writer(path) as writer:
    # connections = [x for x in reader.connections if x.topic == '/ivsensorimu']
    connections = reader.connections

    conn_imu = writer.add_connection('floor1/Imu',
                                     'sad_dataset_msgs/msg/Ivsensorimu',
                                     typestore=typestore)

    conn_localization = writer.add_connection('floor1/Localization',
                                              'sad_dataset_msgs/msg/Localization',
                                              typestore=typestore)

    conn_cansensor = writer.add_connection('floor1/CanSensor',
                                              'sad_dataset_msgs/msg/CanSensor',
                                              typestore=typestore)

    conn_laserscan = writer.add_connection('floor1/LaserScan',
                                              'sensor_msgs/msg/LaserScan',
                                              typestore=typestore)

    for connection, timestamp, rawdata in reader.messages(connections=connections):
         msg = reader.deserialize(rawdata, connection.msgtype)

         if(connection.msgtype == 'locsensor/msg/ivsensorimu'):
             # imu_data.header.frame_id = msg.header.frame_id
             # imu_data.header.stamp.sec = msg.header.stamp.sec
             # imu_data.header.stamp.nanosec = msg.header.stamp.nanosec
             #
             imu_data = Ivsensorimu()
             header_ros1_to_ros2(msg.header, imu_data.header)
             # #
             imu_data.update = msg.update
             imu_data.time_tag = msg.TimeTag
             imu_data.utc_time = msg.utcTime
             imu_data.gyro_x = msg.gyro_x
             imu_data.gyro_y = msg.gyro_y
             imu_data.gyro_z = msg.gyro_z
             imu_data.acce_x = msg.acce_x
             imu_data.acce_y = msg.acce_y
             imu_data.acce_z = msg.acce_z
             imu_data.temperature = msg.Temperature


             outdata: memoryview | bytes = typestore.serialize_cdr(imu_data, conn_imu.msgtype)
             writer.write(
                 conn_imu,
                 timestamp, outdata)

             # print(1)
         elif(connection.msgtype == 'scrubber_slam/msg/localization'):
             # local_data.header.frame_id = msg.header.frame_id
             # local_data.header.stamp.sec = msg.header.stamp.sec
             # local_data.header.stamp.nanosec = msg.header.stamp.nanosec

             header_ros1_to_ros2(msg.header, local_data.header)


             local_data.loaclization_status = msg.loaclization_status

             # pose_ros1_to_ros2(msg.fusion_pose.pose, local_data.fusion_pose.pose)
             cov_pos_ros1_to_ros2(msg.fusion_pose, local_data.fusion_pose)
             # local_data.fusion_pose.covariance = msg.fusion_pose.covariance
             # local_data.fusion_pose.pose.orientation.x = msg.fusion_pose.pose.orientation.x
             # local_data.fusion_pose.pose.orientation.y = msg.fusion_pose.pose.orientation.y
             # local_data.fusion_pose.pose.orientation.z = msg.fusion_pose.pose.orientation.z
             # local_data.fusion_pose.pose.orientation.w = msg.fusion_pose.pose.orientation.w
             # local_data.fusion_pose.pose.position.x = msg.fusion_pose.pose.position.x
             # local_data.fusion_pose.pose.position.y = msg.fusion_pose.pose.position.y
             # local_data.fusion_pose.pose.position.z = msg.fusion_pose.pose.position.z
             # local_data.dr_pose = msg.dr_pose
             cov_pos_ros1_to_ros2(msg.dr_pose, local_data.fusion_pose)
             #
             local_data.velocity = msg.velocity

             local_data.localization_mode = msg.localization_mode
             local_data.region_type = msg.region_type

             outdata: memoryview | bytes = typestore.serialize_cdr(local_data, conn_localization.msgtype)
             writer.write(
                 conn_localization,
                 timestamp, outdata)


         # print(2)
         elif(connection.msgtype == 'canbus_msgs/msg/CanSensor'):
             header_ros1_to_ros2(msg.header, can_data.header)
             can_data.wheel_speed.wheelspeed_f = msg.wheel_speed.wheelspeed_f
             can_data.wheel_speed.wheelspeed_lr = msg.wheel_speed.wheelspeed_lr
             can_data.wheel_speed.wheelspeed_rr = msg.wheel_speed.wheelspeed_rr
             can_data.ultrasonic_radar.uss_status = msg.ultrasonic_radar.uss_status
             can_data.ultrasonic_radar.uss_com_status = msg.ultrasonic_radar.uss_com_status
             can_data.ultrasonic_radar.ult_detector = msg.ultrasonic_radar.ult_detector
             can_data.ultrasonic_radar.ult_detector_indirect = msg.ultrasonic_radar.ult_detector_indirect

             outdata: memoryview | bytes = typestore.serialize_cdr(can_data, conn_cansensor.msgtype)
             writer.write(
                 conn_cansensor,
                 timestamp, outdata)

         # print(3)
         elif(connection.msgtype == 'sensor_msgs/msg/LaserScan'):

             ret = migrate_message(
                 srcts,
                 dstts,
                 'sensor_msgs/msg/LaserScan',
                 'sensor_msgs/msg/LaserScan',
                 {},
                 msg,
             )
             # laser_data = ret[0]

             outdata: memoryview | bytes = typestore.serialize_cdr(ret, conn_laserscan.msgtype)
             writer.write(
                 conn_laserscan,
                 timestamp, outdata)



# def main(args=None):
#     writer = rosbag2_py.SequentialWriter()
#
#     storage_options = rosbag2_py._storage.StorageOptions(
#         uri='/home/zjt/code/slam_ros2/src/sad/data/rosbag2/floor1',
#         storage_id='sqlite3')
#     converter_options = rosbag2_py._storage.ConverterOptions('', '')
#     writer.open(storage_options, converter_options)
#
#     topic_info = rosbag2_py._storage.TopicMetadata(
#         name='synthetic',
#         type='example_interfaces/msg/Int32',
#         serialization_format='cdr')
#     # imu_topic_info = rosbag2_py._storage.TopicMetadata(
#     #     name='locsensor/msg/ivsensorimu',
#     #     type='sensor_msgs/msg/Imu',
#     #     serialization_format='cdr')
#
#     writer.create_topic(topic_info)
#     # writer.create_topic(imu_topic_info)
#
#     time_stamp = Clock().now()
#     for ii in range(0, 100):
#         data = Int32()
#         data.data = ii
#         ss = serialize_message(data)
#         writer.write(
#             'synthetic',
#             serialize_message(data),
#             time_stamp.nanoseconds)
        # time_stamp += Duration(seconds=1)

# if __name__ == '__main__':
#     main()