#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/table.hpp"
#include "table_scan.hpp"
#include "resolve_type.hpp"

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
  const auto& table = _in->get_output();
  const auto chunk_count = table->chunk_count();
  const auto data_type = table->column_type(_column_id);
  auto out_table = std::make_shared<Table>();
  auto pos_list = PosList{};


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
        const auto typed_search_value = type_cast<Type>(_search_value);
        for (auto value_index = ChunkOffset{0}; value_index < values.size(); ++value_index){
          switch (_scan_type) {
            case ScanType::OpEquals:
              if (values[value_index] == typed_search_value) {
                pos_list.push_back(RowID{chunk_index, value_index});
              }
              break;
            case ScanType::OpNotEquals:
              if (values[value_index] != typed_search_value) {
                pos_list.push_back(RowID{chunk_index, value_index});
              }
              break;
            case ScanType::OpLessThan:
              if (values[value_index] < typed_search_value) {
                pos_list.push_back(RowID{chunk_index, value_index});
              }
              break;
            case ScanType::OpLessThanEquals:
              if (values[value_index] <= typed_search_value) {
                pos_list.push_back(RowID{chunk_index, value_index});
              }
              break;
            case ScanType::OpGreaterThan:
              if (values[value_index] > typed_search_value) {
                pos_list.push_back(RowID{chunk_index, value_index});
              }
              break;
            case ScanType::OpGreaterThanEquals:
              if (values[value_index] >= typed_search_value) {
                pos_list.push_back(RowID{chunk_index, value_index});
              }
              break;
          }
        }

      }
    });
  }

  const auto& ref_segment = make_shared<ReferenceSegment>(table, _column_id, std::make_shared<PosList>(pos_list));
  out_table->get_chunk(ChunkID{0})->add_segment(ref_segment);

  return out_table;
}


}  // namespace opossum
