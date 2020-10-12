#include <vtr_storage/data_bubble.hpp>

namespace vtr {
namespace storage {

void DataBubble::reset() {
  unload();
  data_stream_.reset();
}

void DataBubble::load() {
  if (loadFromIndex_ && loadFromTime_)
    throw std::runtime_error{"Cannot have both indices and time set to load."};
  // seek to the start idx
  if (loadFromIndex_)
    loadIndexRange_(indices_.start_index, indices_.stop_index);
  else if (loadFromTime_)
    loadTimeRange_(indices_.start_time, indices_.stop_time);
}

void DataBubble::load(int32_t local_idx) {
  if (loadFromTime_) throw std::runtime_error{"Bubble is in time mode."};
  if (!loadFromIndex_) throw std::runtime_error{"Global index not set."};

  auto anytype_message =
      data_stream_->readAtIndex(indices_.start_index + local_idx);
  if (anytype_message) {
    data_map_.insert({local_idx, *anytype_message});
    if (anytype_message->has_timestamp()) {
      time_map_.insert({anytype_message->get_timestamp(), local_idx});
    }
  }
}

void DataBubble::loadTime(TimeStamp time) {
  if (loadFromIndex_) throw std::runtime_error{"Bubble in index mode."};
  loadFromTime_ = true;
  auto anytype_message = data_stream_->readAtTimestamp(time);
  if (anytype_message) {
    auto local_idx = anytype_message->get_index() - indices_.start_index;
    data_map_.insert({local_idx, *anytype_message});
    time_map_.insert({time, local_idx});
  }
}

void DataBubble::unload() {
  indices_ = ChunkIndices();
  loadFromIndex_ = false;
  loadFromTime_ = false;
  data_map_.clear();
  time_map_.clear();
}

void DataBubble::insert(const VTRMessage& message) {
  if (loadFromIndex_) throw std::runtime_error{"Bubble in index mode."};
  loadFromTime_ = true;
  int32_t idx = data_map_.size();
  data_map_.insert({idx, message});
  if (!message.has_timestamp())
    throw std::runtime_error{"Inserting data without time set is not allowed."};
  time_map_.insert({message.get_timestamp(), idx});
}

VTRMessage DataBubble::retrieve(int32_t local_idx) {
  if (loadFromTime_) throw std::runtime_error{"Bubble in time mode."};
  if (!loadFromIndex_) throw std::runtime_error{"Global index not set."};
  if (!isLoaded(local_idx)) load(local_idx);
  if (!isLoaded(local_idx))
    throw std::out_of_range("DataBubble has no data at this index: " + std::to_string(local_idx));

  return data_map_[local_idx];
}

VTRMessage DataBubble::retrieveTime(TimeStamp time) {
  if (loadFromIndex_) throw std::runtime_error{"Bubble in index mode."};
  if (!isLoaded(time)) loadTime(time);
  if (!isLoaded(time))
    throw std::out_of_range("DataBubble has no data at this time: " + std::to_string(time));

  return data_map_[time_map_[time]];
}

bool DataBubble::setIndices(uint64_t index_begin, uint64_t index_end) {
  if (loadFromTime_) throw std::runtime_error{"Bubble in time mode."};
  if (loadFromIndex_) throw std::runtime_error{"Indices already set."};
  if (index_end < index_begin)  // {
    throw std::invalid_argument{"index_end cannot be less than index_begin: " + std::to_string(index_begin) + ", " + std::to_string(index_end)};

  indices_.start_index = index_begin;
  indices_.stop_index = index_end;
  loadFromIndex_ = true;

  return true;
}

bool DataBubble::setTimeIndices(TimeStamp time_begin, TimeStamp time_end) {
  if (loadFromIndex_) throw std::runtime_error{"Bubble in index mode."};

  indices_.start_time = time_begin;
  indices_.stop_time = time_end;
  loadFromTime_ = true;

  return true;
}

void DataBubble::loadIndexRange_(int32_t global_idx0, int32_t global_idx1) {
  auto anytype_message_vector =
      data_stream_->readAtIndexRange(global_idx0, global_idx1);
  if (anytype_message_vector->size() > 0) {
    for (int32_t local_idx = 0; local_idx <= global_idx1 - global_idx0;
         ++local_idx) {
      auto anytype_message = anytype_message_vector->at(local_idx);
      data_map_.insert({local_idx, *anytype_message});
      if (anytype_message->has_timestamp()) {
        time_map_.insert({anytype_message->get_timestamp(), local_idx});
      }
    }
  }
}

void DataBubble::loadTimeRange_(TimeStamp time0, TimeStamp time1) {
  auto anytype_message_vector =
      data_stream_->readAtTimestampRange(time0, time1);
  if (anytype_message_vector->size() > 0) {
    for (auto anytype_message : *anytype_message_vector) {
      auto time = anytype_message->get_timestamp();
      if (time_map_.find(time) != time_map_.end()) continue;
      int32_t idx = data_map_.size();
      time_map_.insert({time, idx});
    }
  }
}

}  // namespace storage
}  // namespace vtr