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

Table::Table(const ChunkOffset target_chunk_size) {
  this->create_new_chunk();
  this->_target_chunk_size = target_chunk_size;
}

void Table::add_column(const std::string& name, const std::string& type) {
  this->_column_names.push_back(name);
  this->_column_types.push_back(type);
  for (const auto& chunk : this->_chunks) {
    resolve_data_type(type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      chunk->add_segment(std::make_shared<ValueSegment<ColumnDataType>>());
    });
  }
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  // if current chunk size is maxed out, create a new chunk, adding as many segments as we have columns
  if (this->_chunks.back()->size() >= this->target_chunk_size()) {
    this->create_new_chunk();
  }

  this->_chunks.back()->append(values);
}

void Table::create_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>(Chunk{});
  for (const auto& column_type : this->_column_types) {
    resolve_data_type(column_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      new_chunk->add_segment(std::make_shared<ValueSegment<ColumnDataType>>());
    });
  }
  this->_chunks.push_back(new_chunk);
}

ColumnCount Table::column_count() const { return this->_chunks.at(0)->column_count(); }

ChunkOffset Table::row_count() const {
  auto total_rows = ChunkOffset{0};
  for (const auto& chunk : this->_chunks) {
    total_rows += chunk->size();
  }
  return total_rows;
}

ChunkID Table::chunk_count() const { return ChunkID{static_cast<ChunkID>(this->_chunks.size())}; }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto column_name_location = std::find(this->column_names().begin(), this->column_names().end(), column_name);
  Assert(column_name_location != this->column_names().end(), "The given column name is not contained in the table.");
  return ColumnID{static_cast<ColumnID>(std::distance(this->column_names().begin(), column_name_location))};
}

ChunkOffset Table::target_chunk_size() const { return this->_target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return this->_column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return this->_column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return this->_column_types.at(column_id); }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) { return this->_chunks.at(chunk_id); }

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const { return this->_chunks.at(chunk_id); }

void Table::compress_chunk(const ChunkID chunk_id) {
  // Implementation goes here
  Fail("Implementation is missing.");
}

}  // namespace opossum
