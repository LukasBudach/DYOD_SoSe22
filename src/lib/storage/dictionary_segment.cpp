#include <unordered_set>

#include "dictionary_segment.hpp"
#include "type_cast.hpp"

#include "utils/assert.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) {
  auto unique_values = std::unordered_set<T>{};
  const auto& segment_size = abstract_segment->size();

  for (auto value_index = ChunkOffset{0}; value_index < segment_size; ++value_index) {
    const auto& raw_value = type_cast<T>(abstract_segment->operator[](value_index));

    // if value was unseen, it can be inserted into the set -> we know it is a new unique value
    auto was_unseen = unique_values.insert(raw_value).second;
    if (was_unseen) {
      _dictionary.push_back(raw_value);
    }
    _attribute_vector->set(value_index, get_encoded_value(raw_value));
  }
}

template <typename T>
const ValueID DictionarySegment<T>::get_encoded_value(const T& raw_value) const{
  const auto value_index = std::find(_dictionary.begin(), _dictionary.end(), raw_value);
  Assert(value_index != _dictionary.end(), "The value was not found for encoding.");
  return static_cast<ValueID>(std::distance(_dictionary.begin(), value_index));
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
  const auto& n_elements = _attribute_vector->size();
  for (auto index = ValueID{0}; index < n_elements; ++index) {
    if (value_of_value_id(_attribute_vector->get(index)) >= value) {
      return index;
    }
  }

  return n_elements;
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  return lower_bound(type_cast<T>(value));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  const auto& n_elements = _attribute_vector->size();
  for (auto index = ValueID{0}; index < n_elements; ++index) {
    if (value_of_value_id(_attribute_vector->get(index)) <= value) {
      return index;
    }
  }

  return n_elements;
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  return upper_bound(type_cast<T>(value));
}

template <typename T>
ChunkOffset DictionarySegment<T>::unique_values_count() const {
  return static_cast<ChunkOffset>(_dictionary.size());
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
size_t DictionarySegment<T>::estimate_memory_usage() const {
  return size_t{};
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
