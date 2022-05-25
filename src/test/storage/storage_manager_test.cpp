#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageStorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& storage_manager = StorageManager::get();
    auto table_a = std::make_shared<Table>();
    auto table_b = std::make_shared<Table>(4);

    storage_manager.add_table("first_table", table_a);
    storage_manager.add_table("second_table", table_b);
  }
};

TEST_F(StorageStorageManagerTest, GetTable) {
  auto& storage_manager = StorageManager::get();
  auto table_c = storage_manager.get_table("first_table");
  auto table_d = storage_manager.get_table("second_table");
  EXPECT_THROW(storage_manager.get_table("third_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DropTable) {
  auto& storage_manager = StorageManager::get();
  storage_manager.drop_table("first_table");
  EXPECT_THROW(storage_manager.get_table("first_table"), std::exception);
  EXPECT_THROW(storage_manager.drop_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, ResetTable) {
  StorageManager::get().reset();
  auto& storage_manager = StorageManager::get();
  EXPECT_THROW(storage_manager.get_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DoesNotHaveTable) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.has_table("third_table"), false);
}

TEST_F(StorageStorageManagerTest, HasTable) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.has_table("first_table"), true);
}

TEST_F(StorageStorageManagerTest, TableNames) {
  auto& storage_manager = StorageManager::get();
  auto expected_tables = std::vector<std::string>{"first_table", "second_table"};
  auto manager_table_names = storage_manager.table_names();
  // since the return vector could be unsorted, we sort both vectors here to ensure we can properly compare them
  std::sort(expected_tables.begin(), expected_tables.end());
  std::sort(manager_table_names.begin(), manager_table_names.end());
  EXPECT_EQ(manager_table_names, expected_tables);
}

TEST_F(StorageStorageManagerTest, PrintTableInfo) {
  auto& storage_manager = StorageManager::get();
  auto stream = std::stringstream{};

  // this is assuming the expected sorting from the StorageManager::table_names() function
  auto expected_print = std::string{
      "Table Name: first_table\t# Columns: 0\t# Rows: 0\t# Chunks: 1\nTable Name: "
      "second_table\t# Columns: 0\t# Rows: 0\t# Chunks: 1\n"};
  storage_manager.print(stream);
  EXPECT_EQ(stream.str(), expected_print);
}

}  // namespace opossum
