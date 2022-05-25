#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "abstract_segment.hpp"
#include "dictionary_segment.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

// ReferenceSegment is a specific segment type that stores all its values as position list of a referenced column.
class ReferenceSegment : public AbstractSegment {
 public:
  // Creates a reference segment. The parameters specify the positions and the referenced column.
  ReferenceSegment(const std::shared_ptr<const Table>& referenced_table, const ColumnID referenced_column_id,
                   const std::shared_ptr<const PosList>& pos);

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  void append(const AllTypeVariant&) override;

  ChunkOffset size() const override;

  const std::shared_ptr<const PosList>& pos_list() const;

  const std::shared_ptr<const Table>& referenced_table() const;

  ColumnID referenced_column_id() const;

  size_t estimate_memory_usage() const final;
 protected:
  const std::shared_ptr<const Table> _referenced_table{};
  const ColumnID _referenced_column_id{};
  const std::shared_ptr<const PosList> _pos{};
};

}  // namespace opossum
