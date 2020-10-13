#include "vtr_storage/data_stream_reader.hpp"

namespace vtr {
namespace storage {

std::shared_ptr<VTRMessage>
DataStreamReaderBase::fetchCalibration() {
  if (!calibration_fetched_) {
    if (!(base_directory_/CALIBRATION_FOLDER).exists()) {
      throw NoBagExistsException(base_directory_/CALIBRATION_FOLDER);
    }
    calibration_reader_ =
        std::make_shared<RandomAccessReader>(CALIBRATION_FOLDER);

    rosbag2_cpp::StorageOptions calibration_storage_options = storage_options_;
    calibration_storage_options.uri =
        (base_directory_ / CALIBRATION_FOLDER).string();
    calibration_reader_->open(calibration_storage_options,
                              this->converter_options_);
    rclcpp::Serialization<vtr_messages::msg::RigCalibration>
        calibration_serialization;

    auto bag_message = calibration_reader_->read_at_index(1);

    if (bag_message) {
      auto extracted_msg = std::make_shared<vtr_messages::msg::RigCalibration>();
      rclcpp::SerializedMessage serialized_msg;
      rclcpp::SerializedMessage extracted_serialized_msg(
          *bag_message->serialized_data);
      calibration_serialization.deserialize_message(&extracted_serialized_msg,
                                                    extracted_msg.get());
      calibration_fetched_ = true;
      auto anytype_msg = std::make_shared<VTRMessage>(*extracted_msg);
      anytype_msg->set_index(bag_message->database_index);
      if (bag_message->time_stamp != NO_TIMESTAMP_VALUE) {
        anytype_msg->set_timestamp(bag_message->time_stamp);
      }
      calibration_msg_ = anytype_msg;
    } else {
      throw std::runtime_error("calibration database has no messages!");
    }
  }
  return calibration_msg_;
}

}  // namespace storage
}  // namespace vtr