#pragma once

#include <vector>

#include "all_type_variant.hpp"
#include "utils/assert.hpp"

#include "abstract_attribute_vector.hpp"

namespace opossum {

template <typename T>
class FixedWidthIntegerVector : public AbstractAttributeVector {
 public:
  // returns the value id at a given position
  ValueID get(const size_t index) const override;

  // sets the value id at a given position
  void set(const size_t index, const ValueID value_id) override;

  // returns the number of values
  size_t size() const override;

  // returns the width of biggest value id in bytes
  AttributeVectorWidth width() const override;

 protected:
  std::vector<T> _indices{};
};

}  // namespace opossum
