#pragma once

#include <utility>
#include <vtr_messages/msg/rig_calibration.hpp>
#include "vtr_storage/data_stream_base.hpp"
#include "vtr_storage/message.hpp"
#include "vtr_storage/random_access_reader.hpp"

namespace vtr {
namespace storage {

class DataStreamReaderBase : public DataStreamBase {
 public:
  DataStreamReaderBase(const std::string &data_directory,
                       const std::string &stream_name = "")
      : DataStreamBase(data_directory, stream_name) {}
  virtual ~DataStreamReaderBase() {}

  virtual void openAndGetMessageType() = 0;
  virtual void close() = 0;

  virtual std::shared_ptr<VTRMessage> readAtIndex(int32_t index) = 0;
  virtual std::shared_ptr<VTRMessage> readAtTimestamp(
      rcutils_time_point_value_t time) = 0;
  virtual std::shared_ptr<std::vector<std::shared_ptr<VTRMessage>>>
  readAtIndexRange(int32_t index_begin, int32_t index_end) = 0;
  virtual std::shared_ptr<std::vector<std::shared_ptr<VTRMessage>>>
  readAtTimestampRange(rcutils_time_point_value_t time_begin,
                       rcutils_time_point_value_t time_end) = 0;

  std::shared_ptr<VTRMessage> fetchCalibration();

 protected:
  rclcpp::Serialization<vtr_messages::msg::RigCalibration>
      calibration_serialization_;
  std::shared_ptr<RandomAccessReader> calibration_reader_;
  bool calibration_fetched_ = false;
  std::shared_ptr<VTRMessage> calibration_msg_;
};

template <typename MessageType>
class DataStreamReader : public DataStreamReaderBase {
 public:
  DataStreamReader(const std::string &data_directory,
                   const std::string &stream_name = "");
  ~DataStreamReader();

  void openAndGetMessageType() override;
  void close() override;

  // returns a nullptr if no data exist at the specified index/timestamp
  std::shared_ptr<VTRMessage> readAtIndex(int32_t index) override;
  std::shared_ptr<VTRMessage> readAtTimestamp(
      rcutils_time_point_value_t time) override;

  // returns an empty vector if no data exist at the specified range
  std::shared_ptr<std::vector<std::shared_ptr<VTRMessage>>> readAtIndexRange(
      int32_t index_begin, int32_t index_end) override;
  std::shared_ptr<std::vector<std::shared_ptr<VTRMessage>>>
  readAtTimestampRange(rcutils_time_point_value_t time_begin,
                       rcutils_time_point_value_t time_end) override;

  bool seekByIndex(int32_t index);
  bool seekByTimestamp(rcutils_time_point_value_t timestamp);
  std::shared_ptr<VTRMessage> readNextFromSeek();

 protected:
  std::shared_ptr<VTRMessage> convertBagMessage(
      std::shared_ptr<rosbag2_storage::SerializedBagMessage> bag_message);

  rclcpp::Serialization<MessageType> serialization_;
  std::shared_ptr<RandomAccessReader> reader_;
  bool seeked_ = false;
};

}  // namespace storage
}  // namespace vtr

#include "vtr_storage/data_stream_reader.inl"
