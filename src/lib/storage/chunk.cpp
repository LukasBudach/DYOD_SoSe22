#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(const std::shared_ptr<AbstractSegment> segment) { _segments.push_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  Assert(values.size() == _segments.size(), "Row to be inserted has not equally many values as chunk columns!");
  for (ColumnCount column_index{0}; column_index < values.size(); ++column_index) {
    _segments.at(column_index)->append(values.at(column_index));
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const {
  return _segments.at(column_id);
}

ColumnCount Chunk::column_count() const { return ColumnCount{static_cast<ColumnCount>(_segments.size())}; }

ChunkOffset Chunk::size() const {
  if (column_count() > 0) {
    return _segments.at(0)->size();
  } else {
    return ChunkOffset{0};
  }
}

}  // namespace opossum
