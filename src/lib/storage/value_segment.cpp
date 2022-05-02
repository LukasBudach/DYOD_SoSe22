#include "value_segment.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  Assert(chunk_offset < _stored_values.size(), "Tried to access an element out of range!");
  return AllTypeVariant{_stored_values.at(chunk_offset)};
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  _stored_values.push_back(type_cast<T>(val));
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return ChunkOffset{static_cast<ChunkOffset>(_stored_values.size())};
}

template <typename T>
const std::vector<T>& ValueSegment<T>::values() const {
  return _stored_values;
}

template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  const auto datapoint_size = sizeof(T);
  return size() * datapoint_size;
}

// Macro to instantiate the following classes:
// template class ValueSegment<int32_t>;
// template class ValueSegment<int64_t>;
// template class ValueSegment<float>;
// template class ValueSegment<double>;
// template class ValueSegment<std::string>;
EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
