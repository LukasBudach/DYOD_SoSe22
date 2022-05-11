#include <memory>
#include <string>

#include "../base_test.hpp"

#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/dictionary_segment.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public BaseTest {
 protected:
  void SetUp() override {
    for (auto value = int32_t{0}; value <= 10; value += 2) {
      value_segment_int->append(value);
    }

    value_segment_str->append("Bill");
    value_segment_str->append("Steve");
    value_segment_str->append("Alexander");
    value_segment_str->append("Steve");
    value_segment_str->append("Hasso");
    value_segment_str->append("Bill");
  }

  std::shared_ptr<ValueSegment<int32_t>> value_segment_int = std::make_shared<ValueSegment<int32_t>>();
  std::shared_ptr<ValueSegment<std::string>> value_segment_str = std::make_shared<ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_str);
  });

  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<std::string>>(segment);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 4u);

  // Test sorting
  const auto dict = dict_segment->dictionary();
  EXPECT_EQ(dict[0], "Alexander");
  EXPECT_EQ(dict[1], "Bill");
  EXPECT_EQ(dict[2], "Hasso");
  EXPECT_EQ(dict[3], "Steve");
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->lower_bound(4), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(4), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant{4}), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant{4}), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(5), ValueID{3});
  EXPECT_EQ(dict_segment->upper_bound(5), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(15), INVALID_VALUE_ID);
}

// TODO(student): You should add some more tests here (full coverage would be appreciated) and possibly in other files.

TEST_F(StorageDictionarySegmentTest, DirectValueAccess) {
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->operator[](1), AllTypeVariant{2});
  EXPECT_EQ(dict_segment->operator[](3), AllTypeVariant{6});
}

TEST_F(StorageDictionarySegmentTest, GetValueAccess) {
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->get(1), 2);
  EXPECT_EQ(dict_segment->get(3), 6);
}

TEST_F(StorageDictionarySegmentTest, AttributeVectorAccess) {
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_NO_THROW(dynamic_cast<const FixedWidthIntegerVector<uint8_t>&>(*dict_segment->attribute_vector()));
}

TEST_F(StorageDictionarySegmentTest, ValueAtValueIdAccess) {
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->value_of_value_id(ValueID{3}), 6);
  EXPECT_EQ(dict_segment->value_of_value_id(ValueID{5}), 10);
}

TEST_F(StorageDictionarySegmentTest, ExtimateMemoryUsage) {
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->estimate_memory_usage(), 30);
}

TEST_F(StorageDictionarySegmentTest, SixteenBitIntegerVector) {
  value_segment_int = std::make_shared<ValueSegment<int32_t>>();
  for (auto value = int32_t{0}; value <= 257; ++value) {
    value_segment_int->append(value);
  }
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->attribute_vector()->width(), 2);
}

TEST_F(StorageDictionarySegmentTest, ThritytwoBitIntegerVector) {
  value_segment_int = std::make_shared<ValueSegment<int32_t>>();
  for (auto value = int32_t{0}; value <= 65537; ++value) {
    value_segment_int->append(value);
  }
  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->attribute_vector()->width(), 4);
}

}  // namespace opossum
