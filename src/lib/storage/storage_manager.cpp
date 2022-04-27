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
  this->_tables.insert(std::make_pair(name, table));
}

void StorageManager::drop_table(const std::string& name) {
  Assert(this->has_table(name), "The table attempted to be removed does not exist.");
  this->_tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  Assert(this->has_table(name), "The table attempted to be retrieved does not exist.");
  return this->_tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const { return this->_tables.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  auto table_names = std::vector<std::string>{};
  table_names.reserve(this->_tables.size());

  for (const auto& table : this->_tables) {
    table_names.push_back(table.first);
  }

  std::sort(table_names.begin(), table_names.end());

  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  // Implementation goes here(name, #columns, #rows, #chunks)
  auto table_names_sorted = this->table_names();
  for (const auto& table_name : table_names_sorted) {
    auto table = this->get_table(table_name);
    out << "Table Name: " << table_name << "\t# Columns: " << table->column_count()
        << "\t# Rows: " << table->row_count() << "\t# Chunks: " << table->chunk_count() << "\n";
  }
}

void StorageManager::reset() { this->_tables = std::unordered_map<std::string, std::shared_ptr<Table>>{}; }

}  // namespace opossum
