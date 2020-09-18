#pragma once

#include "vtr_storage/data_stream_reader.hpp"

namespace vtr {
namespace storage {

template <typename MessageType>
DataStreamReader<MessageType>::DataStreamReader(
    const std::string &data_directory, const std::string &stream_name)
    : DataStreamReaderBase(data_directory, stream_name) {}

template <typename MessageType>
DataStreamReader<MessageType>::~DataStreamReader() {
  close();
}

template <typename MessageType>
void DataStreamReader<MessageType>::close() {
  reader_.reset();
  this->opened_ = false;
}

template <typename MessageType>
void DataStreamReader<MessageType>::openAndGetMessageType() {
  if (this->opened_ == false) {
    reader_ = std::make_shared<RandomAccessReader>(this->stream_name_);
    reader_->open(this->storage_options_, this->converter_options_);
    this->opened_ = true;
  }
  // ToDo: get message type, specialize this->serialization_ based on message
  // type?
}

template <typename MessageType>
std::shared_ptr<VTRMessage> DataStreamReader<MessageType>::convertBagMessage(std::shared_ptr<rosbag2_storage::SerializedBagMessage> bag_message) {
  auto extracted_msg = std::make_shared<MessageType>();
  rclcpp::SerializedMessage serialized_msg;
  rclcpp::SerializedMessage extracted_serialized_msg(
      *bag_message->serialized_data);
  this->serialization_.deserialize_message(&extracted_serialized_msg,
                                           extracted_msg.get());

  auto anytype_msg = std::make_shared<VTRMessage>(*extracted_msg);
  anytype_msg->set_index(bag_message->database_index);
  if (bag_message->time_stamp != NO_TIMESTAMP_VALUE) {
    anytype_msg->set_timestamp(bag_message->time_stamp);
  }
  return anytype_msg;
}

template <typename MessageType>
std::shared_ptr<VTRMessage> DataStreamReader<MessageType>::readAtIndex(
    int32_t index) {
  openAndGetMessageType();
  auto bag_message = reader_->read_at_index(index);
  return convertBagMessage(bag_message);
}

template <typename MessageType>
std::shared_ptr<VTRMessage> DataStreamReader<MessageType>::readAtTimestamp(
    rcutils_time_point_value_t time) {
  openAndGetMessageType();
  auto bag_message = reader_->read_at_timestamp(time);
  return convertBagMessage(bag_message);
}

template <typename MessageType>
std::shared_ptr<std::vector<std::shared_ptr<VTRMessage>>>
DataStreamReader<MessageType>::readAtIndexRange(int32_t index_begin,
                                                int32_t index_end) {
  openAndGetMessageType();
  auto bag_message_vector =
      reader_->read_at_index_range(index_begin, index_end);
  auto deserialized_bag_message_vector =
      std::make_shared<std::vector<std::shared_ptr<VTRMessage>>>();
  for (auto bag_message : *bag_message_vector) {
    auto anytype_msg = convertBagMessage(bag_message);
    deserialized_bag_message_vector->push_back(
        anytype_msg);  // ToDo: reserve the vector instead of pushing
                       // back
  }
  return deserialized_bag_message_vector;
}

template <typename MessageType>
std::shared_ptr<std::vector<std::shared_ptr<VTRMessage>>>
DataStreamReader<MessageType>::readAtTimestampRange(
    rcutils_time_point_value_t time_begin,
    rcutils_time_point_value_t time_end) {
  openAndGetMessageType();
  auto bag_message_vector =
      reader_->read_at_timestamp_range(time_begin, time_end);
  auto deserialized_bag_message_vector =
      std::make_shared<std::vector<std::shared_ptr<VTRMessage>>>();
  for (auto bag_message : *bag_message_vector) {
    auto anytype_msg = convertBagMessage(bag_message);
    deserialized_bag_message_vector->push_back(
        anytype_msg);  // ToDo: reserve the vector instead of pushing
                       // back
  }
  return deserialized_bag_message_vector;
}

}  // namespace storage
}  // namespace vtr