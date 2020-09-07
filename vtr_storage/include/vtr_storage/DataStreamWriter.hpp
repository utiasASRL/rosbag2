// description
#pragma once

#include "vtr_storage/DataStreamBase.hpp"
#include "vtr_storage/SequentialAppendWriter.hpp"

namespace vtr {
namespace storage {

class DataStreamWriter : public DataStreamBase {
 public:
  DataStreamWriter(const std::string &data_directory_string,
                   const std::string &stream_name, bool append = false);
  ~DataStreamWriter();

  void open();
  void close();

  // returns the id of the inserted message
  int32_t write(const TestMsgT &message);

 protected:
  rosbag2_storage::TopicMetadata createTopicMetadata();

  rosbag2_storage::TopicMetadata tm_;
  std::shared_ptr<SequentialAppendWriter> writer_;
  bool append_;
};
}  // namespace storage
}  // namespace vtr