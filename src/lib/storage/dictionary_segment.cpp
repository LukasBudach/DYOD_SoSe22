#include <unordered_set>

#include "dictionary_segment.hpp"
#include "type_cast.hpp"

#include "utils/assert.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) {
  for (auto index = ChunkOffset{0}; index < abstract_segment->size(); index++) {
    const auto& value = type_cast<T>(abstract_segment->operator[](index));
    _dictionary.push_back(value);
  }

  // use standard library functions to remove duplicate values from dictionary
  // sorted dictionary also allows us to be more efficient in element lookup
  std::sort(begin(_dictionary), end(_dictionary));
  const auto& end_it = std::unique(begin(_dictionary), end(_dictionary));
  _dictionary.erase(end_it, _dictionary.end());
  _dictionary.shrink_to_fit();

  // determine how many bits are needed to encode based on dictionary size
  const auto num_bits = std::ceil(std::log2(_dictionary.size()));
  Assert(num_bits <= 32, "The dictionary is too large for this compression algorithm!");

  // initialize attribute vector with smallest applicable integer type
  if (num_bits <= 8) {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint8_t>>();
  } else if (num_bits <= 16) {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>();
  } else {
    _attribute_vector = std::make_shared<FixedWidthIntegerVector<uint32_t>>();
  }

  // encode segment
  for (auto index = ChunkOffset{0}; index < abstract_segment->size(); ++index) {
    const auto& value = type_cast<T>(abstract_segment->operator[](index));
    _attribute_vector->set(index, get_encoded_value(value));
  }
}

template <typename T>
const ValueID DictionarySegment<T>::get_encoded_value(const T& raw_value) const{
  // "lower_bound" is more efficient than "find" and can be used as dictionary is sorted & immutable
  const auto target_it = std::lower_bound(_dictionary.begin(), _dictionary.end(), raw_value);
  return static_cast<ValueID>(std::distance(_dictionary.begin(), target_it));
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return AllTypeVariant{value_of_value_id(_attribute_vector->get(chunk_offset))};
}

template <typename T>
T DictionarySegment<T>::get(const ChunkOffset chunk_offset) const {
  return value_of_value_id(_attribute_vector->get(chunk_offset));
}

template <typename T>
void DictionarySegment<T>::append(const AllTypeVariant& value) {
  Fail("Dictionary segments are immutable, i.e., values cannot be appended.");
}

template <typename T>
const std::vector<T>& DictionarySegment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const AbstractAttributeVector> DictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
const T DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  return _dictionary.at(value_id);
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const T value) const {
  const auto& target_it = std::lower_bound(begin(_dictionary), end(_dictionary), value);

  if (target_it == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }

  return static_cast<ValueID>(std::distance(_dictionary.begin(), target_it));
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  return lower_bound(type_cast<T>(value));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  const auto& target_it = std::upper_bound(begin(_dictionary), end(_dictionary), value);

  if (target_it == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }

  return static_cast<ValueID>(std::distance(_dictionary.begin(), target_it));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  return upper_bound(type_cast<T>(value));
}

template <typename T>
ChunkOffset DictionarySegment<T>::unique_values_count() const {
  return _dictionary.size();
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
size_t DictionarySegment<T>::estimate_memory_usage() const {
  return _dictionary.size() * sizeof(T) + _attribute_vector->size() * _attribute_vector->width();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
