import shutil
from pathlib import Path

from open3d.cuda.pybind.core import float32
from rosbags.convert.converter import migrate_message
from rosbags.highlevel import AnyReader
from rosbags.rosbag2 import Writer
from rosbags.typesys import Stores, get_typestore, get_types_from_msg

# from nav_msgs.msg import
from sad_dataset_msgs.msg import *
# ulhk message
# GNSS positioning topic: /ublox_gps_node/fix, msg type: NavSatFix
# 3D LiDAR point clouds: /velodyne_points_0, msg type: PointCloud2
# camera: None
# imu /imu/data, msg type: Imu
# span-cpt topic /novatel_data/inspvax, type: Inspvax
from sensor_msgs.msg import NavSatFix, PointCloud2, Imu
from novatel_gps_msgs.msg import Inspvax

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
# bagpath = Path('/media/zjt/T7/sad/2dmapping/floor4.bag')
# bagpath = Path('/media/zjt/T7/sad/ulhk/test2.bag')
bagpath = Path('/media/zjt/T7/sad/ulhk/test3.bag')

imu_topic = '/imu/data'
imu_data = Imu()
imu_msg_type = 'sensor_msgs/msg/Imu'

point_cloud_topic = '/velodyne_points_0'
point_cloud_data = PointCloud2()
point_cloud_msg_type = 'sensor_msgs/msg/PointCloud2'

gnss_data_topic = '/ublox_gps_node/fix'
gnss_data = NavSatFix()
gnss_msg_type = 'sensor_msgs/msg/NavSatFix'

ins_topic = '/novatel_data/inspvax'
ins_data = Inspvax()
# set ins_msg_type to output(which means ros2 version msg type)
ins_msg_type = 'novatel_gps_msgs/msg/Inspvax'
# ins_msg_type = 'novatel_msgs/msg/INSPVAX' #ros1 version

# imu_data = Ivsensorimu()
# local_data = Localization()
# can_data = CanSensor()
# laser_data = LaserScan()

# Create a type store to use if the bag has no message definitions.
# typestore = get_typestore(Stores.ROS2_HUMBLE)
# typestore = get_typestore(Stores.ROS1_NOETIC)

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
         '/home/zjt/code/slam_ros2/src/sad_msgs/msg/FaultInfo.msg',
         '/opt/ros/humble/share/novatel_gps_msgs/msg/Inspvax.msg',
         '/opt/ros/humble/share/novatel_gps_msgs/msg/NovatelReceiverStatus.msg',
         '/opt/ros/humble/share/novatel_gps_msgs/msg/NovatelExtendedSolutionStatus.msg',
         '/opt/ros/humble/share/novatel_gps_msgs/msg/NovatelMessageHeader.msg',
         '/home/zjt/code/python/novatel_msgs/msg/INSPVAX.msg',
         '/home/zjt/code/python/novatel_msgs/msg/CommonHeader.msg']

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
# path = Path('/home/zjt/code/slam_ros2/src/sad/data/rosbag2/floor4')
path = Path('/home/zjt/code/slam_ros2/src/sad/data/rosbag2/ulhk/test2')
path = Path('/home/zjt/code/slam_ros2/src/sad/data/rosbag2/ulhk/test3')


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

    conn_imu = writer.add_connection(imu_topic,
                                     imu_msg_type,
                                     typestore=typestore)

    conn_point_cloud = writer.add_connection(point_cloud_topic,
                                     point_cloud_msg_type,
                                     typestore=typestore)

    conn_gnss = writer.add_connection(gnss_data_topic,
                                     gnss_msg_type,
                                     typestore=typestore)

    conn_ins = writer.add_connection(ins_topic,
                                      ins_msg_type,
                                      typestore=typestore)


    for connection, timestamp, rawdata in reader.messages(connections=connections):
        msg = reader.deserialize(rawdata, connection.msgtype)

        if (connection.msgtype == imu_msg_type):
            ret = migrate_message(
                srcts,
                dstts,
                'sensor_msgs/msg/Imu',
                'sensor_msgs/msg/Imu',
                {},
                msg,
            )


            outdata: memoryview | bytes = typestore.serialize_cdr(ret, conn_imu.msgtype)
            writer.write(
                conn_imu,
                timestamp, outdata)

        elif (connection.msgtype == point_cloud_msg_type):
            ret = migrate_message(
                srcts,
                dstts,
                'sensor_msgs/msg/PointCloud2',
                'sensor_msgs/msg/PointCloud2',
                {},
                msg,
            )

            outdata: memoryview | bytes = typestore.serialize_cdr(ret, conn_point_cloud.msgtype)
            writer.write(
                conn_point_cloud,
                timestamp, outdata)

        elif (connection.msgtype == gnss_msg_type):
            ret = migrate_message(
                srcts,
                dstts,
                'sensor_msgs/msg/NavSatFix',
                'sensor_msgs/msg/NavSatFix',
                {},
                msg,
            )

            outdata: memoryview | bytes = typestore.serialize_cdr(ret, conn_gnss.msgtype)
            writer.write(
                conn_gnss,
                timestamp, outdata)
        elif (connection.msgtype == 'novatel_msgs/msg/INSPVAX'): # set to ros1 version for read from bags

            # header
            ins_data.header.frame_id = str(msg.header.id)
            ins_data.header.stamp.sec = timestamp // 1000000000
            ins_data.header.stamp.nanosec = timestamp % 1000000000

            # novatel message header
            ins_data.novatel_msg_header.gps_week_num = msg.header.gps_week
            ins_data.novatel_msg_header.gps_seconds = float(msg.header.gps_week_seconds)

            # ins_status
            ins_data.ins_status = str(msg.ins_status)

            # position_type
            ins_data.position_type = str(msg.position_type)

            # data
            ins_data.latitude = msg.latitude
            ins_data.longitude = msg.longitude
            ins_data.altitude = msg.altitude

            ins_data.undulation = msg.undulation

            ins_data.north_velocity = msg.north_velocity
            ins_data.east_velocity = msg.east_velocity
            ins_data.up_velocity = msg.up_velocity

            ins_data.roll = msg.roll
            ins_data.pitch = msg.pitch
            ins_data.azimuth = ins_data.azimuth

            ins_data.latitude_std = msg.latitude_std
            ins_data.longitude_std = msg.longitude_std
            ins_data.altitude_std = msg.altitude_std


            ins_data.north_velocity_std = msg.north_velocity_std
            ins_data.east_velocity_std = msg.east_velocity_std
            ins_data.up_velocity_std = msg.up_velocity_std

            ins_data.roll_std = msg.roll_std
            ins_data.pitch_std = msg.pitch_std
            ins_data.azimuth_std = msg.azimuth_std

            # seconds_since_update
            ins_data.seconds_since_update = msg.seconds_since_update

            outdata: memoryview | bytes = typestore.serialize_cdr(ins_data, conn_ins.msgtype)
            writer.write(
                conn_ins,
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
