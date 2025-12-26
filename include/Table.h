#ifndef _SQLDB_TABLE_H_
#define _SQLDB_TABLE_H_

#include "ColumnType.h"
#include "Cursor.h"
#include "Log.h"

#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <vector>
#include <string_view>
#include <string>
#include <numeric>

#include "robin_hood.h"

namespace sqldb {  
  class Table {
  public:
    Table() : log_(std::make_shared<Log>()) { }
    Table(std::vector<ColumnType> key_type)
      : key_type_(std::move(key_type)), log_(std::make_shared<Log>()) { }
    
    virtual ~Table() { }
    
    virtual std::unique_ptr<Cursor> seekBegin(int sheet = 0) = 0;
    virtual std::unique_ptr<Cursor> seek(const Key & key) = 0;
    virtual std::unique_ptr<Cursor> seek(int row, int sheet = 0) { return std::unique_ptr<Cursor>(nullptr); }

    virtual std::unique_ptr<Cursor> insert(const Key & key) = 0;
    virtual std::unique_ptr<Cursor> insert(int sheet = 0) = 0;
    virtual std::unique_ptr<Cursor> increment(const Key & key) = 0;
    virtual std::unique_ptr<Cursor> assign(std::vector<int> columns) = 0;
    virtual void remove(const Key & key) = 0;
    
    std::unique_ptr<Cursor> assign() {
      // Select all columns
      std::vector<int> cols(getNumFields());
      std::iota(cols.begin(), cols.end(), 0);
      return assign(std::move(cols));
    }    
    
    virtual std::unique_ptr<Table> copy() const = 0;
    virtual void addColumn(std::string_view name, sqldb::ColumnType type, bool nullable = true, bool unique = false, int decimals = -1) = 0;
    virtual void clear() = 0;

    virtual int getNumSheets() const { return 1; }
    virtual int getNumFields(int sheet = 0) const = 0;
    virtual ColumnType getColumnType(int column_index, int sheet = 0) const = 0;
    virtual bool isColumnNullable(int column_index, int sheet = 0) const = 0;
    virtual bool isColumnUnique(int column_index, int sheet = 0) const = 0;
    virtual const std::string & getColumnName(int column_index, int sheet = 0) const = 0;
    virtual int getColumnDecimals(int column_index) const { return 0; }
    
    virtual void begin() { }
    virtual void commit() { }
    virtual void rollback() { }

    void append(Table & other) {
      // TODO: handle multiple sheets
      // TODO: handle column mismatch
      if (!getNumFields()) {
	setKeyType(other.getKeyType());
	for (int i = 0; i < other.getNumFields(); i++) {
	  addColumn(other.getColumnName(i), other.getColumnType(i), other.isColumnNullable(i), other.isColumnUnique(i), other.getColumnDecimals(i));
	}
      }
      
      if (auto cursor = other.seekBegin()) {
	int n = 0;
	do {	  
	  if (n == 0) begin();
	  auto my_cursor = insert(cursor->getRowKey());
	  for (int i = 0; i < cursor->getNumFields(); i++) {
	    bool is_null = cursor->isNull(i);
	    switch (cursor->getColumnType(i)) {
	    case sqldb::ColumnType::BOOL:
	    case sqldb::ColumnType::ENUM:
	    case sqldb::ColumnType::INTEGER:
	    case sqldb::ColumnType::DATETIME:
	    case sqldb::ColumnType::DATE:
	      my_cursor->bind(cursor->getInteger(i), !is_null);
	      break;
	    case sqldb::ColumnType::DOUBLE:
	      my_cursor->bind(cursor->getDouble(i), !is_null);
	      break;
	    case sqldb::ColumnType::ANY:
	    case sqldb::ColumnType::TEXT:
	    case sqldb::ColumnType::URL:
	    case sqldb::ColumnType::TEXT_KEY:
	    case sqldb::ColumnType::BINARY_KEY:
	    case sqldb::ColumnType::CHAR:
	    case sqldb::ColumnType::VARCHAR:
	      my_cursor->bind(cursor->getText(i), !is_null);
	      break;

	    case sqldb::ColumnType::BLOB:
	    case sqldb::ColumnType::VECTOR:
	      my_cursor->bind("", false); // not implemented
	      break;
	    }
	  }
	  my_cursor->execute();
	  if (++n == 4096) {
	    commit();
	    n = 0;
	  }
	} while (cursor->next());
	if (n) commit();

	getLog().append(other.getLog());
      }
    }

    int getColumnByNames(std::unordered_set<std::string> names, int sheet = 0) const {
      for (int i = getNumFields(sheet) - 1; i >= 0; i--) {
	if (names.count(getColumnName(i, sheet))) return i;
      }
      return -1;
    }

    int getColumnByName(std::string_view name, int sheet = 0) const {
      for (int i = getNumFields(sheet) - 1; i >= 0; i--) {
	if (getColumnName(i, sheet) == name) return i;
      }
      return -1;
    }

    int getColumnByType(ColumnType type, int sheet = 0) const {
      for (int i = 0, n = getNumFields(sheet); i < n; i++) {
	if (getColumnType(i, sheet) == type) return i;
      }
      return -1;
    }

    std::vector<int> getColumnsByNames(std::unordered_set<std::string> names, int sheet = 0) const {
      std::vector<int> r;
      for (int i = getNumFields(sheet) - 1; i >= 0; i--) {
	if (names.count(getColumnName(i)) > 0) {
	  r.push_back(i);
	}
      }
      return r;
    }
    
    void addIntegerColumn(std::string_view name, bool nullable = true, bool unique = false) { addColumn(std::move(name), ColumnType::INTEGER, nullable, unique); }
    void addCharColumn(std::string_view name) { addColumn(std::move(name), ColumnType::CHAR); }
    void addDateTimeColumn(std::string_view name) { addColumn(std::move(name), ColumnType::DATETIME); }
    void addDateColumn(std::string_view name) { addColumn(std::move(name), ColumnType::DATE); }
    void addVarCharColumn(std::string_view name, bool nullable = true, bool unique = false) { addColumn(std::move(name), ColumnType::VARCHAR, unique); }
    void addTextColumn(std::string_view name) { addColumn(std::move(name), ColumnType::TEXT); }
    void addDoubleColumn(std::string_view name, bool nullable = true, bool unique = false, int decimals = -1) { addColumn(std::move(name), ColumnType::DOUBLE, nullable, unique, decimals); }
    void addURLColumn(std::string_view name) { addColumn(std::move(name), ColumnType::URL); }
    void addTextKeyColumn(std::string_view name) { addColumn(std::move(name), ColumnType::TEXT_KEY); }
    void addBinaryKeyColumn(std::string_view name) { addColumn(std::move(name), ColumnType::BINARY_KEY); }
    void addEnumColumn(std::string_view name) { addColumn(std::move(name), ColumnType::ENUM); }
    void addBoolColumn(std::string_view name, bool nullable = true, bool unique = false) { addColumn(std::move(name), ColumnType::BOOL, nullable, unique); }
    void addBlobColumn(std::string_view name) { addColumn(std::move(name), ColumnType::BLOB); }
    
    std::string dumpRow(const Key & key) {
      std::string r;
      if (auto cursor = seek(key)) {
	for (int i = 0; i < cursor->getNumFields(); i++) {
	  if (i) r += ";";
	  r += cursor->getText(i);
	}
      } else {
	r += "not found";
      }
      return r;
    }

    bool hasNumericKey() const noexcept {
      return key_type_.size() == 1 && is_numeric(key_type_.front());
    }

    void setHasHumanReadableKey(bool t) noexcept { has_human_readable_key_ = t; }
    bool hasHumanReadableKey() const noexcept { return has_human_readable_key_; }
    
    const std::vector<ColumnType> & getKeyType() const noexcept { return key_type_; }
    void setKeyType(std::vector<ColumnType> key_type) { key_type_ = std::move(key_type); }
    size_t getKeySize() const noexcept { return key_type_.size(); }
    
    bool hasFilter(int col) const noexcept {
      return filter_.count(col) != 0;
    }

    void clearFilter(int col) {
      filter_.erase(col);
    }
    void setFilter(int col, robin_hood::unordered_flat_set<sqldb::Key> keys) {
      filter_.emplace(col, std::move(keys));
    }
    
    const std::unordered_map<int, robin_hood::unordered_flat_set<sqldb::Key>> & getFilter() const noexcept { return filter_; }
    
    const Log & getLog() const noexcept { return *log_; }
    Log & getLog() noexcept { return *log_; }

    static inline std::string empty_string;
    
  private:
    std::vector<ColumnType> key_type_;
    bool has_human_readable_key_ = false;
    std::unordered_map<int, robin_hood::unordered_flat_set<sqldb::Key> > filter_;
    std::shared_ptr<Log> log_;
  };
};

#endif
