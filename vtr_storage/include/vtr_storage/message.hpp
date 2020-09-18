#pragma once

#include <rcutils/time.h>
#include <any>
#include <boost/optional.hpp>
#include <iostream>

namespace vtr {
namespace storage {

constexpr rcutils_time_point_value_t NO_TIMESTAMP_VALUE =
    -1;  // timestamp value stored in sqlite database if message has no
         // timestamps

class VTRMessage {
 public:
  VTRMessage() = default;
  template <class T>
  VTRMessage(T message) : message_{message} {
    static_assert(!std::is_same_v<std::any, T>,
                  "Attempted to initialize a VTRMessage with an std::any!");
  }

  virtual ~VTRMessage() = default;

  template <class T>
  VTRMessage& operator=(const T& message) {
    message_ = std::make_any<T>(message);
    return *this;
  }

  template <class T>
  void set(T message) {
    message_ = std::make_any<T>(message);
  }

  template <class T>
  T get() const {
    try {
      return std::any_cast<T>(message_);
    } catch (const std::bad_any_cast& e) {
      throw std::runtime_error(
          "Any cast failed in retrieving data in VTR Storage");
    }
  }

  rcutils_time_point_value_t get_timestamp() const {
    if (timestamp_ == boost::none) {
      throw std::runtime_error(
          "Attempted to get uninitialized timestamp of a VTRMessage");
    } else {
      return timestamp_.get();
    }
  }

  bool has_timestamp() const { return timestamp_ != boost::none; }

  void set_timestamp(rcutils_time_point_value_t new_timestamp) {
    timestamp_ = new_timestamp;
  }

  int32_t get_index() const {
    if (database_index_ == boost::none) {
      throw std::runtime_error(
          "Attempted to get uninitialized timestamp of a VTRMessage");
    } else {
      return database_index_.get();
    }
  }

  bool has_index() const { return database_index_ != boost::none; }

  void set_index(int32_t new_index) { database_index_ = new_index; }

 private:
  std::any message_;
  boost::optional<int32_t> database_index_;
  boost::optional<rcutils_time_point_value_t> timestamp_;
};

}  // namespace storage
}  // namespace vtr