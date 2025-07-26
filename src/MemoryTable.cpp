#include <MemoryTable.h>
#include <Cursor.h>

#include <map>
#include <unordered_map>
#include <cassert>
#include <mutex>
#include <charconv>

using namespace std;
using namespace sqldb;

namespace sqldb {
  class MemoryTableCursor;
};

class sqldb::MemoryStorage {
public:
  friend class sqldb::MemoryTableCursor;
  
  MemoryStorage() { }

  std::unique_ptr<Cursor> seek(const Key & key);
  std::unique_ptr<Cursor> seekBegin();
  std::unique_ptr<MemoryTableCursor> insertOrUpdate(const Key & key);
  std::unique_ptr<MemoryTableCursor> insertOrUpdate();
  std::unique_ptr<Cursor> increment(const Key & key);
  std::unique_ptr<Cursor> assign(std::vector<int> columns);

  void remove(const Key & key) {
    std::lock_guard<std::mutex> guard(mutex_);
    data_.erase(key);
  }

  void addColumn(std::string_view name, sqldb::ColumnType type, bool unique, int decimals) {
    std::lock_guard<std::mutex> guard(mutex_);
    header_row_.push_back(std::tuple(type, std::string(name), unique, decimals));
  }
  
  int getNumFields() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return static_cast<int>(header_row_.size());
  }
  int getNumRows() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return static_cast<int>(data_.size());
  }

  ColumnType getColumnType(int column_index) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<0>(header_row_[idx]) : ColumnType::ANY;
  }

  const std::string & getColumnName(int column_index) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<1>(header_row_[idx]) : null_string;
  }

  bool isColumnUnique(int column_index) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<2>(header_row_[idx]) : false;
  }

  int getColumnDecimals(int column_index) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<3>(header_row_[idx]) : false;
  }

  void clear() {
    std::lock_guard<std::mutex> guard(mutex_);
    data_.clear();    
  }

  size_t size() const { return data_.size(); }

private:
  // use ordered map for iterator stability
  std::map<Key, std::vector<std::string> > data_;
  std::vector<std::tuple<ColumnType, std::string, bool, int> > header_row_;
  long long auto_increment_ = 0;
  mutable std::mutex mutex_;

  static inline std::string null_string;
};

class sqldb::MemoryTableCursor : public Cursor {
public:
  MemoryTableCursor(MemoryStorage * storage,
		    std::map<Key, std::vector<std::string> >::iterator it,
		    bool is_increment_op = false)
    : storage_(storage), header_row_(storage->header_row_), it_(it), is_increment_op_(is_increment_op) {
    updateRowKey();
  }
  MemoryTableCursor(MemoryStorage * storage,
		    Key pending_key,
		    bool is_increment_op = false)
    : storage_(storage), header_row_(storage->header_row_), pending_key_(std::move(pending_key)), is_increment_op_(is_increment_op) { }
  MemoryTableCursor(MemoryStorage * storage,
		    std::vector<int> selected_columns
		    )
    : storage_(storage), header_row_(storage->header_row_), selected_columns_(std::move(selected_columns)), is_increment_op_(false) { }

  size_t execute() override {    
    std::lock_guard<std::mutex> guard(storage_->mutex_);
    auto & data = storage_->data_;
    if (!pending_key_.empty()) {
      auto [ it, is_new ] = data.emplace(std::move(pending_key_), std::vector<std::string>());
      it_ = std::move(it);
      pending_key_.clear();
    }
    if (it_ != data.end()) {
      auto & v = it_->second;
      if (is_increment_op_) {
	for (auto [ key, value ] : pending_row_) {
	  if (v.size() <= static_cast<size_t>(key)) v.resize(static_cast<size_t>(key) + 1);
	  auto & v0 = v[key];
	  if (v0.empty()) {
	    v0 = value;
	  } else if (is_numeric(getColumnType(key))) {
	    v0 = std::to_string(stoi(v0) + stoi(value));
	  }
	}      
      } else {
	for (auto [ key, value ] : pending_row_) {
	  if (v.size() <= static_cast<size_t>(key)) v.resize(static_cast<size_t>(key) + 1);
	  v[key] = value;
	}
      }
      pending_row_.clear();
      return 1;
    } else {
      return 0;
    }
  }

  size_t update(const Key & key) override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);
    auto & data = storage_->data_;
    auto it = data.find(key);
    if (it != data.end()) {
      auto & v = it->second;

      for (size_t i = 0; i < selected_columns_.size(); i++) {
	auto col = static_cast<size_t>(selected_columns_[i]);
	auto it = pending_row_.find(static_cast<int>(i));
	if (it != pending_row_.end()) {
	  if (col >= v.size()) v.resize(col + 1);
	  v[col] = it->second;
	} else if (col < v.size()) {
	  v[col].clear();
	}
      }
      pending_row_.clear();
      return 1;
    } else {
      return 0;
    }
  }
  
  void set(int column_idx, string_view value, bool is_defined = true) override {
    if (is_defined) {
      pending_row_[column_idx] = value;
    } else {
      pending_row_.erase(column_idx);
    }
  }

  void set(int column_idx, int value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, long long value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, double value, bool is_defined = true) override { set(column_idx, std::to_string(value), is_defined); }
  void set(int column_idx, const void * data, size_t len, bool is_defined = true) override { set(column_idx, std::string_view(reinterpret_cast<const char *>(data), len), is_defined); }

  bool next() override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    if (it_ != data.end() && ++it_ != data.end()) {
      updateRowKey();
      return true;
    } else {
      return false;
    }
  }

  std::string_view getText(int column_index) override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    if (column_index >= 0 && it_ != data.end()) {
      auto idx = static_cast<size_t>(column_index);
      auto & row = it_->second;
      if (idx < row.size()) return row[idx];
    }
    return null_string;    
  }
    
  std::vector<uint8_t> getBlob(int column_index) override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    std::vector<uint8_t> r;
    if (column_index >= 0 && it_ != data.end()) {
      auto idx = static_cast<size_t>(column_index);
      auto & row = it_->second;
      if (idx < row.size()) {
	auto & v = row[idx];
	r.reserve(v.size());
	for (size_t i = 0; i < v.size(); i++) r.push_back(static_cast<uint8_t>(v[i]));
      }
    }
    return r;
  }
    
  int getNumFields() const override {
    return static_cast<int>(header_row_.size());
  }

  ColumnType getColumnType(int column_index) const override {
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<0>(header_row_[idx]) : ColumnType::ANY;
  }
    
  const std::string & getColumnName(int column_index) override {
    auto idx = static_cast<size_t>(column_index);
    return idx < header_row_.size() ? std::get<1>(header_row_[idx]) : null_string;
  }
    
  bool isNull(int column_index) const override {
    std::lock_guard<std::mutex> guard(storage_->mutex_);

    auto & data = storage_->data_;
    if (column_index >= 0 && it_ != data.end()) {
      auto idx = static_cast<size_t>(column_index);
      auto & row = it_->second;
      if (idx < row.size()) return row[idx].empty();
    }
    return true;
  }

  long long getLastInsertId() const override {
    return last_insert_id_;
  }

  void setLastInsertId(long long id) { last_insert_id_ = id; }

  double getDouble(int column_index, double default_value = 0.0) override {
    auto s = getText(column_index);
    if (!s.empty()) {
      double d;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), d);
      if (ec == std::errc()) return d;
    }
    return default_value;
  }

  float getFloat(int column_index, float default_value = 0.0f) override {
    auto s = getText(column_index);
    if (!s.empty()) {
      float f;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), f);
      if (ec == std::errc()) return f;	
    }
    return default_value;
  }

  long long getInteger(int column_index, long long default_value = 0) override {
    auto s = getText(column_index);
    if (!s.empty()) {
      long long ll;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), ll);
      if (ec == std::errc()) return ll;	
    }
    return default_value;  
  }

  Key getKey(int column_index) override {
    auto type = getColumnType(column_index);
    if (type == ColumnType::ANY) {
      auto s = getText(column_index);
      long long ll;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), ll);
      return ec == std::errc() ? Key(ll) : Key(s);
    } else if (is_numeric(type)) {
      return Key(getInteger(column_index));
    } else {
      return Key(getText(column_index));
    }
  }

protected:
  void updateRowKey() {
    setRowKey(it_->first);
  }

private:
  MemoryStorage* storage_;
  std::vector<std::tuple<ColumnType, std::string, bool, int> > header_row_;
  std::map<Key, std::vector<std::string> >::iterator it_;
  Key pending_key_;
  std::unordered_map<int, std::string> pending_row_;
  std::vector<int> selected_columns_;
  bool is_increment_op_;
  long long last_insert_id_ = 0;

  static inline std::string null_string;
};

std::unique_ptr<Cursor>
MemoryStorage::seek(const Key & key) {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = data_.find(key);
  if (it != data_.end()) {
    return std::make_unique<MemoryTableCursor>(this, move(it));
  } else {
    return std::unique_ptr<Cursor>(nullptr);
  }
}

std::unique_ptr<Cursor>
MemoryStorage::seekBegin() {
  std::lock_guard<std::mutex> guard(mutex_);
  auto it = data_.begin();
  if (it != data_.end()) {
    return std::make_unique<MemoryTableCursor>(this, move(it));
  } else {
    return std::unique_ptr<Cursor>(nullptr);
  }
}

std::unique_ptr<MemoryTableCursor>
MemoryStorage::insertOrUpdate(const Key & key) {
  assert(!key.empty());
  return std::make_unique<MemoryTableCursor>(this, key);
}

std::unique_ptr<MemoryTableCursor>
MemoryStorage::insertOrUpdate() {
  long long id = 0;
  {
    std::lock_guard<std::mutex> guard(mutex_);
    id = ++auto_increment_;
  }
  auto cursor = insertOrUpdate(sqldb::Key(id));
  cursor->setLastInsertId(id);
  return cursor;
}
  
std::unique_ptr<Cursor>
MemoryStorage::increment(const Key & key) {
  assert(!key.empty());
  return std::make_unique<MemoryTableCursor>(this, key, true);
}

std::unique_ptr<Cursor>
MemoryStorage::assign(std::vector<int> columns) {
  return std::make_unique<MemoryTableCursor>(this, columns);
}
 
MemoryTable::MemoryTable()
  : storage_(make_shared<MemoryStorage>()) { }

MemoryTable::MemoryTable(std::vector<ColumnType> key_type)
  : Table(std::move(key_type)), storage_(make_shared<MemoryStorage>()) {
}

void
MemoryTable::addColumn(std::string_view name, sqldb::ColumnType type, bool unique, int decimals) {
  storage_->addColumn(std::move(name), type, unique, decimals);
}

std::unique_ptr<Cursor>
MemoryTable::insert(const Key & key) {
  return storage_->insertOrUpdate(key);
}

std::unique_ptr<Cursor>
MemoryTable::insert(int sheet) {
  return storage_->insertOrUpdate();
}

std::unique_ptr<Cursor>
MemoryTable::increment(const Key & key) {
  return storage_->increment(key);
}

std::unique_ptr<Cursor>
MemoryTable::assign(std::vector<int> columns) {
  return storage_->assign(columns);
}

void
MemoryTable::remove(const Key & key) {
  return storage_->remove(key);
}

std::unique_ptr<Cursor>
MemoryTable::seekBegin(int sheet) {
  return storage_->seekBegin(); 
}

std::unique_ptr<Cursor>
MemoryTable::seek(const Key & key) {
  return storage_->seek(key);
}

int
MemoryTable::getNumFields(int sheet) const {
  return storage_->getNumFields();
}

ColumnType
MemoryTable::getColumnType(int column_index, int sheet) const {
  return storage_->getColumnType(column_index);
}

const std::string &
MemoryTable::getColumnName(int column_index, int sheet) const {
  return storage_->getColumnName(column_index);
}

bool
MemoryTable::isColumnUnique(int column_index, int sheet) const {
  return storage_->isColumnUnique(column_index);
}

int
MemoryTable::getColumnDecimals(int column_index) const {
  return storage_->getColumnDecimals(column_index);
}

void
MemoryTable::clear() {
  storage_->clear();
}
