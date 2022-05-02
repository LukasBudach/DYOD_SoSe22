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
  const auto& n_columns = column_count();

  // required guard, as a smaller row would not cause issues when using the .at accessor on the vector
  Assert(values.size() == n_columns, "The row you're trying to insert does not match the number of columns expected.");

  for (auto column_index = ColumnCount{0}; column_index < n_columns; ++column_index) {
    _segments[column_index]->append(values.at(column_index));
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const { return _segments.at(column_id); }

ColumnCount Chunk::column_count() const { return static_cast<ColumnCount>(_segments.size()); }

ChunkOffset Chunk::size() const {
  if (_segments.empty()) {
    return 0;
  } else {
    return _segments[0]->size();
  }
}

}  // namespace opossum
