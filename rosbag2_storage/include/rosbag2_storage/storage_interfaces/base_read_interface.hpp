// Copyright 2018 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ROSBAG2_STORAGE__STORAGE_INTERFACES__BASE_READ_INTERFACE_HPP_
#define ROSBAG2_STORAGE__STORAGE_INTERFACES__BASE_READ_INTERFACE_HPP_

#include <memory>
#include <string>
#include <vector>

#include "rosbag2_storage/serialized_bag_message.hpp"
#include "rosbag2_storage/topic_metadata.hpp"
#include "rosbag2_storage/visibility_control.hpp"

namespace rosbag2_storage
{
namespace storage_interfaces
{

class ROSBAG2_STORAGE_PUBLIC BaseReadInterface
{
public:
  virtual ~BaseReadInterface() = default;

  virtual bool has_next() = 0;

  virtual std::shared_ptr<SerializedBagMessage> read_next() = 0;

  virtual std::shared_ptr<SerializedBagMessage>
  read_at_timestamp(rcutils_time_point_value_t timestamp)
  {
    timestamp++;
    return nullptr;
  }

  virtual std::shared_ptr<SerializedBagMessage>
  read_at_index(int32_t index) {index++; return nullptr;}

  virtual std::shared_ptr<std::vector<std::shared_ptr<rosbag2_storage::SerializedBagMessage>>>
  read_at_timestamp_range(
    rcutils_time_point_value_t timestamp_begin,
    rcutils_time_point_value_t timestamp_end)
  {
    // dummy code
    timestamp_begin++;
    timestamp_end++;
    return nullptr;
  }

  virtual std::shared_ptr<std::vector<std::shared_ptr<rosbag2_storage::SerializedBagMessage>>>
  read_at_index_range(int32_t index_begin, int32_t index_end)
  {
    // dummy code
    index_begin++;
    index_end++;
    return nullptr;
  }

  virtual bool seek_by_index(int32_t index) {index++; return false;}

  virtual bool seek_by_timestamp(rcutils_time_point_value_t timestamp)
  {
    timestamp++;
    return false;
  }

  virtual std::shared_ptr<rosbag2_storage::SerializedBagMessage>
  modified_read_next() {return nullptr;}

  virtual std::vector<TopicMetadata> get_all_topics_and_types() = 0;
};

}  // namespace storage_interfaces
}  // namespace rosbag2_storage

#endif  // ROSBAG2_STORAGE__STORAGE_INTERFACES__BASE_READ_INTERFACE_HPP_
