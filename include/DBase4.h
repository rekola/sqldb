#ifndef _SQLDB_DBASE4_H_
#define _SQLDB_DBASE4_H_

#include "Table.h"
#include "Cursor.h"

#include <stdexcept>
#include <memory>
#include <unordered_map>

namespace sqldb {
  class DBase4File;
  
  class DBase4 : public Table {
  public:
    DBase4(std::string filename, int primary_key = -1);
    DBase4(const DBase4 & other);
    DBase4(DBase4 && other);

    std::unique_ptr<Table> copy() const override { return std::make_unique<DBase4>(*this); }
       
    int getNumFields(int sheet) const override;
    const std::string & getColumnName(int column_index, int sheet) const override;
    ColumnType getColumnType(int column_index, int sheet) const override;

    bool isColumnNullable(int column_index, int sheet = 0) const override { return true; }
    bool isColumnUnique(int column_index, int sheet = 0) const override { return false; }

    void clear() override {
      throw std::runtime_error("dBase4 is read-only");
    }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool nullable, bool unique, int decimals) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> insert(const Key & key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> insert(int sheet) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> increment(const Key & key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> assign(std::vector<int> columns) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    void remove(const Key & key) override {
      throw std::runtime_error("dBase4 is read-only");
    }

    std::unique_ptr<Cursor> seekBegin(int sheet) override { return seek(0); }
    std::unique_ptr<Cursor> seek(const Key & key) override;
    std::unique_ptr<Cursor> seek(int row, int sheet = 0) override;

    void setPrimaryKeyMapping(std::unordered_map<sqldb::Key, int> m) { primary_key_mapping_ = std::move(m); }

  private:    
    // dbf_ is shared with cursors
    std::shared_ptr<DBase4File> dbf_;
    std::unordered_map<sqldb::Key, int> primary_key_mapping_;
  };
};

#endif

