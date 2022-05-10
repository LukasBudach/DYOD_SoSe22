#include "fixed_width_integer_vector.hpp"

namespace opossum {

template <typename T>
ValueID FixedWidthIntegerVector<T>::get(const size_t index) const {
  return static_cast<ValueID>(_indices.at(index));
}

template <typename T>
void FixedWidthIntegerVector<T>::set(const size_t index, const ValueID value_id) {
  Assert(index <= size(), "You can only set existing values or one beyond the last element for extension purposes!");

  if (index == size()) {
    _indices.push_back(value_id);
  } else {
    _indices[index] = value_id;
  }
}

template <typename T>
size_t FixedWidthIntegerVector<T>::size() const {
  return _indices.size();
}

template <typename T>
AttributeVectorWidth FixedWidthIntegerVector<T>::width() const {
  return sizeof(T);
}

BOOST_PP_SEQ_FOR_EACH(EXPLICIT_INSTANTIATION, FixedWidthIntegerVector, (uint8_t)(u_int16_t)(u_int32_t))

}  // namespace opossum
