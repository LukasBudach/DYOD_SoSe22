#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "table_scan.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
          const AllTypeVariant search_value)
: _in(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value)
{}

ColumnID TableScan::column_id() const {
  return _column_id;
}

ScanType TableScan::scan_type() const {
  return _scan_type;
}

const AllTypeVariant& TableScan::search_value() const {
  return _search_value;
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  auto _filter = [this](AllTypeVariant given_value, AllTypeVariant search_value, std::shared_ptr<std::vector<RowID>> pos_list, ChunkID chunk_index, ChunkOffset value_index, auto type) {
    using Type = typename decltype(type)::type;

    const auto typed_search_value = type_cast<Type>(_search_value);
    const auto typed_given_value = type_cast<Type>(given_value);

    switch (_scan_type) {
      case ScanType::OpEquals:
        if (typed_given_value == typed_search_value) {
          pos_list->push_back(RowID{chunk_index, value_index});
        }
        break;
      case ScanType::OpNotEquals:
        if (typed_given_value != typed_search_value) {
          pos_list->push_back(RowID{chunk_index, value_index});
        }
        break;
      case ScanType::OpLessThan:
        if (typed_given_value < typed_search_value) {
          pos_list->push_back(RowID{chunk_index, value_index});
        }
        break;
      case ScanType::OpLessThanEquals:
        if (typed_given_value <= typed_search_value) {
          pos_list->push_back(RowID{chunk_index, value_index});
        }
        break;
      case ScanType::OpGreaterThan:
        if (typed_given_value > typed_search_value) {
          pos_list->push_back(RowID{chunk_index, value_index});
        }
        break;
      case ScanType::OpGreaterThanEquals:
        if (typed_given_value >= typed_search_value) {
          pos_list->push_back(RowID{chunk_index, value_index});
        }
        break;
    }
  };

  auto table = _in->get_output();
  const auto chunk_count = table->chunk_count();
  const auto data_type = table->column_type(_column_id);
  auto out_table = std::make_shared<Table>();
  auto pos_list = std::make_shared<PosList>();

  // figure out whether this is only one chunk with a reference segment (i.e. operator output) or something else
  const auto is_op_output = (chunk_count == 1) && (std::dynamic_pointer_cast<ReferenceSegment>(table->get_chunk(ChunkID{0})->get_segment(_column_id)));

  /*
   * If input table is operator output:
   *   want to iterate not over all chunks in table, but all chunks referenced by reference segment
   *   within each of those chunks, want to get the segment corresponding to _column_id
   *   cast the segment to its correct type (dictionary/value)
   *   compare the referenced value(s) in the segment against the given search value
   *   keep only those references in pos_list that are actually useful (add to new pos_list)
   * Else:
   *   iterate over all chunks in the input table
   *   get segment corresponding to _column_id in each chunk and cast it
   *   compare all values in the segment against given search value
   *   generate references to all values that match filter condition
   */

  if (is_op_output) {
    const auto& ref_segment = std::dynamic_pointer_cast<ReferenceSegment>(table->get_chunk(ChunkID{0})->get_segment(_column_id));
    table = ref_segment->referenced_table();
    auto referenced_chunks = std::unordered_set<ChunkID>{};
    for (const auto reference : *ref_segment->pos_list()) {
      referenced_chunks.emplace(reference.chunk_id);
    }

    for (auto chunk_index : referenced_chunks) {
      // get the current chunk and the segment corresponding to the _column_id we filter on
      const auto& current_chunk = table->get_chunk(chunk_index);
      const auto& segment = current_chunk->get_segment(_column_id);

      // type magic happening in here; careful!
      resolve_data_type(data_type, [&] (auto type) {
        using Type = typename decltype(type)::type;
        const auto& dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
        if (dict_segment_ptr) {
          const auto& values = dict_segment_ptr->attribute_vector();
        } else {
          const auto& value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
          const auto& values = value_segment_ptr->values();
          for (const auto reference : *ref_segment->pos_list()) {
            if (reference.chunk_id == chunk_index) {
              _filter(values[reference.chunk_offset], _search_value, pos_list, chunk_index, reference.chunk_offset, type);
            }
          }
        }
      });
    }
  } else {
    for (auto chunk_index = ChunkID(0); chunk_index < chunk_count; ++chunk_index) {
      const auto& current_chunk = table->get_chunk(chunk_index);
      const auto& segment = current_chunk->get_segment(_column_id);

      resolve_data_type(data_type, [&] (auto type) {
        using Type = typename decltype(type)::type;
        const auto& dict_segment_ptr = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
        if (dict_segment_ptr) {
          const auto& values = dict_segment_ptr->attribute_vector();
        } else {
          const auto& value_segment_ptr = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
          const auto& values = value_segment_ptr->values();
          for (auto value_index = ChunkOffset{0}; value_index < values.size(); ++value_index) {
            _filter(values[value_index], _search_value, pos_list, chunk_index, value_index, type);
          }
        }
      });
    }
  }

  const auto column_count = table->column_count();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    const auto& ref_segment = make_shared<ReferenceSegment>(table, column_id, pos_list);
    out_table->add_column_definition(table->column_name(column_id), table->column_type(column_id));
    out_table->get_chunk(ChunkID{0})->add_segment(ref_segment);
  }

  return out_table;
}


}  // namespace opossum
