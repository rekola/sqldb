#ifndef _SQLDB_MEMORYTABLE_H_
#define _SQLDB_MEMORYTABLE_H_

#include <Table.h>

#include <memory>

namespace sqldb {
  class MemoryStorage;
  
  class MemoryTable : public Table {
  public:
    MemoryTable();
    MemoryTable(std::vector<ColumnType> key_type);

    std::unique_ptr<Table> copy() const override { return std::make_unique<MemoryTable>(*this); }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool nullable, bool unique, int decimals) override;
    
    std::unique_ptr<Cursor> insert(const Key & key) override;
    std::unique_ptr<Cursor> insert(int sheet = 0) override;
    std::unique_ptr<Cursor> increment(const Key & key) override;
    std::unique_ptr<Cursor> assign(std::vector<int> columns) override;
    void remove(const Key & key) override;
    
    std::unique_ptr<Cursor> seekBegin(int sheet = 0) override;    
    std::unique_ptr<Cursor> seek(const Key & key) override;
    
    int getNumFields(int sheet = 0) const override;
    
    ColumnType getColumnType(int column_index, int sheet) const override;
    const std::string & getColumnName(int column_index, int sheet) const override;
    bool isColumnNullable(int column_index, int sheet) const override;
    bool isColumnUnique(int column_index, int sheet) const override;
    int getColumnDecimals(int column_index) const override;
        
    void clear() override;
    
  private:
    std::shared_ptr<MemoryStorage> storage_;
  };
};

#endif
