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
  return _stored_values[chunk_offset];
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  _stored_values.push_back(type_cast<T>(val));
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return _stored_values.size();
}

template <typename T>
const std::vector<T>& ValueSegment<T>::values() const {
  return _stored_values;
}

template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  return _stored_values.capacity() * sizeof(T);
}

// Macro to instantiate the following classes:
// template class ValueSegment<int32_t>;
// template class ValueSegment<int64_t>;
// template class ValueSegment<float>;
// template class ValueSegment<double>;
// template class ValueSegment<std::string>;
EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
