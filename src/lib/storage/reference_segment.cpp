#include <unordered_set>

#include "reference_segment.hpp"
#include "type_cast.hpp"

#include "utils/assert.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table>& referenced_table,
                                   const ColumnID referenced_column_id, const std::shared_ptr<const PosList>& pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos(pos) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  const auto position = (*_pos)[chunk_offset];
  const auto chunk_index = position.chunk_id;
  const auto value_index = position.chunk_offset;

  return _referenced_table->get_chunk(chunk_index)->get_segment(_referenced_column_id)->operator[](value_index);
}

void ReferenceSegment::append(const AllTypeVariant&) { throw std::logic_error("ReferenceSegment is immutable"); }

ChunkOffset ReferenceSegment::size() const { return _pos->size(); }

const std::shared_ptr<const PosList>& ReferenceSegment::pos_list() const { return _pos; }

const std::shared_ptr<const Table>& ReferenceSegment::referenced_table() const { return _referenced_table; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

size_t ReferenceSegment::estimate_memory_usage() const {
  return sizeof(_referenced_table) + sizeof(_referenced_column_id) + sizeof(_pos);
}

}  // namespace opossum
