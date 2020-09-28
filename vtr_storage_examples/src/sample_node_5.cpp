#include <cstdio>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "rcpputils/filesystem_helper.hpp"
#include "rcutils/time.h"

#include "vtr_logging/logging_init.hpp"
#include "vtr_storage/message.hpp"
#include "vtr_storage/data_bubble.hpp"
#include "vtr_storage/data_stream_reader.hpp"
#include "vtr_storage/data_stream_writer.hpp"
#include <vtr_messages/msg/rig_calibration.hpp>

#include "test_msgs/msg/basic_types.hpp"

// sample code showing how to write/fetch calibration
int main() {
  using TestMsgT = test_msgs::msg::BasicTypes;
  TestMsgT test_msg;

  std::string base_url = "/home/daniel/test/ROS2BagFileParsing/dev_ws/test_rosbag2_writer_api_bag";
  std::string stream_name = "test_stream";

  // write a dummy calibration
  vtr_messages::msg::RigCalibration calibration_msg;
  vtr_messages::msg::CameraCalibration intrinsics;
  intrinsics.k = {1, 0, 0, 0, 1, 0, 0, 0, 1};
  calibration_msg.intrinsics[0] = intrinsics;
  vtr::storage::DataStreamWriterCalibration calibration_writer(base_url);
  calibration_writer.write(calibration_msg);

  // write data
  vtr::storage::DataStreamWriter<TestMsgT> writer(base_url, stream_name);
  for (int i = 1; i <= 10; i++) {
    test_msg.float64_value = i * 10;
    writer.write(vtr::storage::VTRMessage(test_msg));
  }

  // read calibration and data
  vtr::storage::DataStreamReader<TestMsgT> reader(base_url, stream_name);
  std::shared_ptr<vtr_messages::msg::RigCalibration> calibration = reader.fetchCalibration();
  for (auto num : calibration->intrinsics[0].k) {
    std::cout << num << std::endl;
  }
  auto bag_message_vector = reader.readAtIndexRange(1, 9);
  std::cout << "~~~~~~~~~Data~~~~~~~~~~"  << std::endl;
  for (auto message : *bag_message_vector) {
    std::cout << message->template get<TestMsgT>().float64_value << std::endl;
  }

  auto anytype_msg = reader.readAtIndex(2);
  std::cout << (anytype_msg.get() == nullptr) << std::endl;
  auto anytype_msg2 = reader.readAtIndex(9000);
  std::cout << (anytype_msg2.get() == nullptr) << std::endl;
}