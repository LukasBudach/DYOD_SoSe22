#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} { create_new_chunk(); }

void Table::add_column(const std::string& name, const std::string& type) {
  auto guard = std::lock_guard<std::mutex>(_chunk_access_mutex);
  // can't use row_count function here, as it will also get a lock, running us into a deadlock
  Assert((_chunks.size() == 1) && (_chunks.back()->size() == 0), "You can only add a new column to an empty table.");
  _column_names.push_back(name);
  _column_types.push_back(type);
  // we can add the column to only the last chunk we know, as it should be the only one
  // even if there are multiple empty chunks, we would only ever start filling the last one, so ignore all others here
  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    _chunks.back()->add_segment(std::make_shared<ValueSegment<ColumnDataType>>());
  });
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  // Implementation goes here
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  auto guard = std::lock_guard<std::mutex>(_chunk_access_mutex);
  // if current chunk size is maxed out, create a new chunk, adding as many segments as we have columns
  if (_chunks.back()->size() >= target_chunk_size()) {
    // Since we already hold the lock on the _chunks vector, we call the unsafe function here to avoid deadlock.
    _create_new_chunk_unsafe();
  }

  _chunks.back()->append(values);
}

void Table::_create_new_chunk_unsafe() {
  auto new_chunk = std::make_shared<Chunk>();
  for (const auto& column_type : _column_types) {
    resolve_data_type(column_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      new_chunk->add_segment(std::make_shared<ValueSegment<ColumnDataType>>());
    });
  }
  _chunks.push_back(new_chunk);
}

void Table::create_new_chunk() {
  auto guard = std::lock_guard<std::mutex>(_chunk_access_mutex);
  _create_new_chunk_unsafe();
}

ColumnCount Table::column_count() const { return static_cast<ColumnCount>(_column_names.size()); }

ChunkOffset Table::row_count() const {
  auto total_rows = ChunkOffset{0};
  auto guard = std::lock_guard<std::mutex>(_chunk_access_mutex);

  for (const auto& chunk : _chunks) {
    total_rows += chunk->size();
  }
  return total_rows;
}

ChunkID Table::chunk_count() const {
  auto guard = std::lock_guard<std::mutex>(_chunk_access_mutex);
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto column_name_location = std::find(column_names().begin(), column_names().end(), column_name);
  Assert(column_name_location != column_names().end(), "The given column name is not contained in the table.");
  return static_cast<ColumnID>(std::distance(column_names().begin(), column_name_location));
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_types.at(column_id); }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) {
  auto guard = std::lock_guard<std::mutex>(_chunk_access_mutex);
  return _chunks.at(chunk_id);
}

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const {
  auto guard = std::lock_guard<std::mutex>(_chunk_access_mutex);
  return _chunks.at(chunk_id);
}

void Table::compress_chunk(const ChunkID chunk_id) {
  auto thread_pool = std::vector<std::thread>{_column_types.size()};
  const auto& raw_chunk = get_chunk(chunk_id);  // get_chunk performs range check, so we are safe
  auto compressed_chunk = std::make_shared<Chunk>();
  auto compressed_segments = std::vector<std::shared_ptr<AbstractSegment>>{_column_types.size()};
  auto segments_vec_mutex = std::mutex{};

  auto compress_segment = [&raw_chunk, &compressed_segments, &segments_vec_mutex](const auto segment_type,
                                                                                  const auto column_index) {
    resolve_data_type(segment_type, [&](const auto data_type_t) {
      // figure out the type of the segment
      using ColumnDataType = typename decltype(data_type_t)::type;
      auto guard = std::lock_guard<std::mutex>(segments_vec_mutex);
      // add compressed segment to the map of segment (column) name to segment to later add to chunk in correct order
      compressed_segments[column_index] =
          std::make_shared<DictionarySegment<ColumnDataType>>(raw_chunk->get_segment(column_index));
    });
  };

  // create one worker per segment, they will start running the moment they are created
  for (auto column_index = ColumnID{0}; column_index < _column_types.size(); ++column_index) {
    thread_pool[column_index] = std::thread(compress_segment, _column_types[column_index], column_index);
  }

  // wait for all threads to finish execution
  for (auto&& thread : thread_pool) {
    thread.join();
  }

  // add the compressed segments to the chunk in correct order
  for (auto column_index = ColumnID{0}; column_index < _column_types.size(); ++column_index) {
    compressed_chunk->add_segment(compressed_segments[column_index]);
  }

  auto guard = std::lock_guard<std::mutex>(_chunk_access_mutex);
  // replace existing chunk with the new, compressed one
  _chunks[chunk_id] = compressed_chunk;
  /*
   * We currently believe the following is the case:
   * The chunk at chunk_id is guaranteed to be full and therefore already immutable.
   * Anyone who still has an old pointer to the uncompressed chunk still gets the same information as someone with a pointer to the compressed chunk.
   * We ensure noone is trying to get this chunk while we exchange the pointer through the _chunk_access_mutex.
   *
   * Therefore, simply setting the pointer new and letting the shared_pointer do the cleanup once nobody is looking at
   * the old, uncompressed chunk anymore should be safe.
   */
}

}  // namespace opossum
