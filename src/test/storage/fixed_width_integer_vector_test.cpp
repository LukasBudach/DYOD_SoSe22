#include <memory>
#include <string>

#include "../base_test.hpp"

#include "storage/fixed_width_integer_vector.hpp"

namespace opossum {

class FixedWidthIntegerVectorTest : public BaseTest {
};

TEST_F(FixedWidthIntegerVectorTest, SetOverwriteExisting) {
  auto vec = FixedWidthIntegerVector<uint8_t>{};

  vec.set(0, ValueID{1});
  vec.set(1, ValueID{2});
  vec.set(2, ValueID{1});

  vec.set(1, ValueID{3});

  // Test attribute_vector size
  EXPECT_EQ(vec.get(1), ValueID{3});
}


}  // namespace opossum
