#include "storage_manager.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static auto instance = StorageManager{};
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  const bool insert_success = _tables.insert(std::make_pair(name, table)).second;
  Assert(insert_success, name + " already exists, choose a different name!");
}

void StorageManager::drop_table(const std::string& name) {
  const auto values_erased = _tables.erase(name);
  Assert(values_erased, name + " does not exist and cannot be deleted.");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  auto table_names = std::vector<std::string>{};
  table_names.reserve(_tables.size());

  for (const auto& table : _tables) {
    table_names.push_back(table.first);
  }

  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  auto sorted_table_names = table_names();
  // sort the vector of table names, both for better user output (sorted vs randomly ordered tables) and for easy tests
  std::sort(sorted_table_names.begin(), sorted_table_names.end());
  for (const auto& table_name : sorted_table_names) {
    auto table = get_table(table_name);
    out << "Table Name: " << table_name << "\t# Columns: " << table->column_count()
        << "\t# Rows: " << table->row_count() << "\t# Chunks: " << table->chunk_count() << "\n";
  }
}

void StorageManager::reset() { StorageManager::get() = StorageManager{}; }

}  // namespace opossum
