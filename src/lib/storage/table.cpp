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

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} { create_new_chunk(); }

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(row_count() == 0, "You can only add a new column to an empty table.");
  _column_names.push_back(name);
  _column_types.push_back(type);
  // we can add the column to only the last chunk we know, as it should be the only one
  // even if there are multiple empty chunks, we would only ever start filling the last one, so ignore all others here
  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    _chunks.back()->add_segment(std::make_shared<ValueSegment<ColumnDataType>>());
  });
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  // if current chunk size is maxed out, create a new chunk, adding as many segments as we have columns
  if (_chunks.back()->size() >= target_chunk_size()) {
    create_new_chunk();
  }

  _chunks.back()->append(values);
}

void Table::create_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>();
  for (const auto& column_type : _column_types) {
    resolve_data_type(column_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      new_chunk->add_segment(std::make_shared<ValueSegment<ColumnDataType>>());
    });
  }
  _chunks.push_back(new_chunk);
}

ColumnCount Table::column_count() const { return static_cast<ColumnCount>(_column_names.size()); }

ChunkOffset Table::row_count() const {
  auto total_rows = ChunkOffset{0};
  for (const auto& chunk : _chunks) {
    total_rows += chunk->size();
  }
  return total_rows;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto column_name_location = std::find(column_names().begin(), column_names().end(), column_name);
  Assert(column_name_location != column_names().end(), "The given column name is not contained in the table.");
  return static_cast<ColumnID>(std::distance(column_names().begin(), column_name_location));
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_types.at(column_id); }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) { return _chunks.at(chunk_id); }

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const { return _chunks.at(chunk_id); }

void Table::compress_chunk(const ChunkID chunk_id) {
  // Implementation goes here
  Fail("Implementation is missing.");
}

}  // namespace opossum
