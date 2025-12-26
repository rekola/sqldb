#ifndef _SQLDB_CSV_H_
#define _SQLDB_CSV_H_

#include "Table.h"
#include "Cursor.h"

#include <stdexcept>

namespace sqldb {
  class CSVFile;
  
  class CSV : public Table {
  public:
    CSV(std::string csv_file, bool has_records = true);
    CSV(const CSV & other);
    CSV(CSV && other);

    std::unique_ptr<Table> copy() const override { return std::make_unique<CSV>(*this); }
       
    int getNumFields(int sheet = 0) const override;
    const std::string & getColumnName(int column_index, int sheet = 0) const override;

    ColumnType getColumnType(int column_index, int sheet = 0) const override {
      return sheet == 0 && (column_index >= 0 && column_index < getNumFields(sheet)) ? ColumnType::TEXT : ColumnType::ANY;
    }
    
    bool isColumnNullable(int column_index, int sheet = 0) const override { return true; }
    bool isColumnUnique(int column_index, int sheet = 0) const override { return false; }

    void clear() override {
      throw std::runtime_error("CSV is read-only");
    }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool nullable, bool unique, int decimals) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> insert(const Key & key) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> insert(int sheet = 0) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> increment(const Key & key) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> assign(std::vector<int> columns) override {
      throw std::runtime_error("CSV is read-only");
    }
    
    void remove(const Key & key) override {
      throw std::runtime_error("CSV is read-only");
    }

    std::unique_ptr<Cursor> seekBegin(int sheet = 0) override { return seek(0, sheet); }
    std::unique_ptr<Cursor> seek(const Key & key) override;
    std::unique_ptr<Cursor> seek(int row, int sheet = 0) override;
    
  private:
    // csv_ is shared with cursors
    std::vector<std::shared_ptr<CSVFile>> csv_;
  };
};

#endif
