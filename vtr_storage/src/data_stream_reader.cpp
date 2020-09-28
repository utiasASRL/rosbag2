#include "vtr_storage/data_stream_reader.hpp"

namespace vtr {
namespace storage {

std::shared_ptr<vtr_messages::msg::RigCalibration> DataStreamReaderBase::fetchCalibration() {
  if (!calibration_fetched_) {
    calibration_reader_ = std::make_shared<RandomAccessReader>(CALIBRATION_FOLDER);

    rosbag2_cpp::StorageOptions calibration_storage_options = storage_options_;
    calibration_storage_options.uri = (base_directory_ / CALIBRATION_FOLDER).string();
    calibration_reader_->open(calibration_storage_options, this->converter_options_);
    rclcpp::Serialization<vtr_messages::msg::RigCalibration> calibration_serialization;

    auto bag_message = calibration_reader_->read_at_index(1);
    auto extracted_msg = std::make_shared<vtr_messages::msg::RigCalibration>();
    rclcpp::SerializedMessage serialized_msg;
    rclcpp::SerializedMessage extracted_serialized_msg(
        *bag_message->serialized_data);
    calibration_serialization.deserialize_message(&extracted_serialized_msg,
                                            extracted_msg.get());
    assert(extracted_msg.get() != nullptr);
    calibration_msg_ = extracted_msg;
    calibration_fetched_ = true;
  }

  return calibration_msg_;
}


}  // namespace storage
}  // namespace vtr