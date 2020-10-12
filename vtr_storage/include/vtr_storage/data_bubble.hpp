#pragma once

#include <map>
#include "rcutils/types.h"
#include "vtr_logging/logging.hpp"
#include "vtr_messages/msg/time_stamp.hpp"
#include "vtr_storage/data_stream_reader.hpp"

namespace vtr {
namespace storage {

using TimeStamp = rcutils_time_point_value_t;
using VTRTimeStamp = vtr_messages::msg::TimeStamp;

/// \brief Bubble indices into a robochunk stream.
struct ChunkIndices {
  /// \brief Start index into the stream.
  int32_t start_index = 0;
  /// \brief Stop index into the stream.
  int32_t stop_index = 0;
  /// \brief Start time into the stream.
  TimeStamp start_time = 0;
  /// \brief Stop time into the stream.
  TimeStamp stop_time = 0;
};

/// \brief DataBubble class. Container class for a specified range of messages
/// in a robochunk stream.
///        Allows for access and storage of messages in memory.
class DataBubble {
  using DataMap = std::map<int32_t, VTRMessage>;
  using TimeMap = std::map<TimeStamp, int32_t>;

 public:
  DataBubble() : loadFromIndex_(false), loadFromTime_(false) {}

  virtual ~DataBubble() { reset(); }

  /// \brief Initializes a data bubble with a data stream.
  /// \param A pointer to the associated data stream.
  void initialize(std::shared_ptr<DataStreamReaderBase> data_stream) {
    data_stream_ = data_stream;
  }

  void reset();

  /// \brief loads all of the messages associated with this bubble into memory.
  void load();

  /// \brief loads the specified message based on local index
  /// \details This is a local index (i.e. If the bubble wraps 20 messages then
  /// the local index is in the range (0-19))
  /// \param The local index.
  void load(int32_t local_idx);

  /// \brief loads a specific message based on a time tag into memory.
  /// \param time The time stamp of the message to be loaded.
  void loadTime(TimeStamp time);
  void load(VTRTimeStamp time) { return loadTime(toTimeStamp(time)); }

  /// \brief unloads all data associated with the vertex.
  void unload();

  /// \brief Gets the size of the bubble (number of messages)
  /// \return the size of the bubble.
  int32_t size() const { return data_map_.size(); }

  /// \brief Checks to see if a message is loaded based on index.
  /// \return true if the message is loaded, false otherwise.
  bool isLoaded(int32_t idx) { return data_map_.count(idx); };
  bool isLoaded(VTRTimeStamp time) { return isLoaded(toTimeStamp(time)); }

  /// \brief Checks to see if a message is loaded based on time.
  /// \return true if the message is loaded, false otherwise.
  bool isLoaded(TimeStamp time) {
    auto time_it = time_map_.find(time);  // get the index from the time map
    return time_it != time_map_.end() && isLoaded(time_it->second);
  }

  /// \brief Inserts a message into the bubble.
  void insert(const VTRMessage& message);

  /// \brief Retrieves a reference to the message.
  /// \param the index of the message.
  VTRMessage retrieve(int32_t local_idx);
  VTRMessage retrieve(VTRTimeStamp time) {
    return retrieveTime(toTimeStamp(time));
  }

  /// \brief Retrieves a reference to the message.
  /// \param The timestamp of the message.
  VTRMessage retrieveTime(TimeStamp time);

  /// \brief Sets the indicies for this bubble.
  /// \param The start index of this bubble.
  /// \param The end index of this bubble.
  bool setIndices(uint64_t index_begin, uint64_t index_end);
  bool setIndices(VTRTimeStamp time_begin, VTRTimeStamp time_end) {
    return setTimeIndices(toTimeStamp(time_begin), toTimeStamp(time_end));
  }

  /// \brief Sets the Time indices for this bubble.
  /// \param The start time of this bubble.
  /// \param The end time of this bubble.
  bool setTimeIndices(TimeStamp time_begin, TimeStamp time_end);

  /// \brief provides an iterator to the begining of the bubble.
  /// \return Begin iterator into the bubble's data.
  DataMap::iterator begin() { return data_map_.begin(); }

  /// \brief provides an iterator to the end of the bubble.
  /// \brief Begin iterator into the bubble's data.
  DataMap::iterator end() { return data_map_.end(); }

 private:
  /// \brief loads the specified messages into memory based on an index range.
  /// \details These are global indices of messages in the database
  /// \param The start index.
  /// \param The end index.
  void loadIndexRange_(int32_t global_idx0, int32_t global_idx1);

  /// \brief loads a range of messages based on time tags into memory.
  /// \param time Begining time stamp of the message to be loaded.
  /// \param time End time stamp of the message to be loaded.
  void loadTimeRange_(TimeStamp time0, TimeStamp time1);

  TimeStamp toTimeStamp(const VTRTimeStamp& time) {
    return static_cast<TimeStamp>(time.nanoseconds_since_epoch);
  }
  /// \brief A pointer to the Robochunk stream.
  std::shared_ptr<DataStreamReaderBase> data_stream_;
  /// \brief The indices associated with this bubble.
  ChunkIndices indices_;
  /// \brief flag to determine if this bubble can be loaded by index.
  bool loadFromIndex_;
  /// \brief flag to determine if this bubble can be loaded by time.
  bool loadFromTime_;
  /// \brief The map of currently loaded data.
  DataMap data_map_;
  /// \brief maps timestamps to indices in the data map.
  TimeMap time_map_;
};
}  // namespace storage
}  // namespace vtr
