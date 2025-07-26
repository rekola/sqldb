#include "SQLite.h"

#include "SQLException.h"

#include <cassert>
#include <vector>
#include <charconv>

using namespace sqldb;

class SQLiteStatement : public SQLStatement {
public:
  SQLiteStatement(sqlite3 * db, sqlite3_stmt * stmt) : db_(db), stmt_(stmt) {
    assert(db_);
    assert(stmt_);

    num_columns_ = sqlite3_column_count(stmt_);
  }
  ~SQLiteStatement() {
    if (stmt_) sqlite3_finalize(stmt_);
  }
  
  size_t execute() override {
    step();
    return getAffectedRows();
  }
  bool next() override {
    SQLStatement::reset();
    step();
    return results_available_;
  }
  void reset() override {
    SQLStatement::reset();
    
    int r = sqlite3_reset(stmt_);
    
    switch (r) {
    case SQLITE_OK: return;
      
    case SQLITE_SCHEMA:
      throw SQLException(SQLException::SCHEMA_CHANGED, sqlite3_errmsg(db_));
      return;
      
    default:
      assert(0);
      return;
    }
  }
  
  void set(int column_idx, std::string_view value, bool is_defined) override {
    int r;
    if (is_defined) r = sqlite3_bind_text(stmt_, column_idx + 1, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT);
    else r = sqlite3_bind_null(stmt_, column_idx + 1);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }

  void set(int column_idx, int value, bool is_defined) override {
    int r;
    if (is_defined) r = sqlite3_bind_int(stmt_, column_idx + 1, value);
    else r = sqlite3_bind_null(stmt_, column_idx + 1);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }

  void set(int column_idx, double value, bool is_defined) override {
    int r;
    if (is_defined) r = sqlite3_bind_double(stmt_, column_idx + 1, value);
    else r = sqlite3_bind_null(stmt_, column_idx + 1);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }

  void set(int column_idx, long long value, bool is_defined) override {
    int r;
    if (is_defined) r = sqlite3_bind_int64(stmt_, column_idx + 1, (sqlite_int64)value);
    else r = sqlite3_bind_null(stmt_, column_idx + 1);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
    
  void set(int column_idx, const void * data, size_t len, bool is_defined) override {
    int r;
    if (is_defined) r = sqlite3_bind_blob(stmt_, column_idx + 1, data, len, SQLITE_TRANSIENT);
    else r = sqlite3_bind_null(stmt_, column_idx + 1);
    if (r != SQLITE_OK) {
      throw SQLException(SQLException::BIND_FAILED, sqlite3_errmsg(db_));
    }
  }
  
  double getDouble(int column_index, double default_value = 0.0) override {
    if (!isNull(column_index)) {
      return sqlite3_column_double(stmt_, column_index);
    }
    return default_value;
  }
  float getFloat(int column_index, float default_value) override {
    if (!isNull(column_index)) {
      return static_cast<float>(sqlite3_column_double(stmt_, column_index));
    }
    return default_value;
  }
  long long getInteger(int column_index, long long default_value = 0) override {
    if (!isNull(column_index)) {
      return sqlite3_column_int64(stmt_, column_index);
    }
    return default_value;
  }
  std::string_view getText(int column_index) override {
    if (results_available_) {
      auto s = (const char *)sqlite3_column_text(stmt_, column_index);
      if (s) {
	auto len = (size_t)sqlite3_column_bytes(stmt_, column_index);
	return std::string_view(s, len);
      }
    }
    return null_string;
  }
  std::vector<uint8_t> getBlob(int column_index) override {
    std::vector<uint8_t> r;
    if (results_available_) {
      auto data = reinterpret_cast<const uint8_t*>(sqlite3_column_blob(stmt_, column_index));
      auto len = sqlite3_column_bytes(stmt_, column_index);
      r.reserve(len);
      for (int i = 0; i < len; i++) r.push_back(data[i]);
    }
    return r;
  }
  Key getKey(int column_index) override {
    auto type = getColumnType(column_index);
    if (type == ColumnType::ANY) {
      // FIXME: get actual type
      auto s = getText(column_index);
      long long ll;
      auto [ ptr, ec ] = std::from_chars(s.data(), s.data() + s.size(), ll);
      return ec == std::errc() ? Key(ll) : Key(s);
    } else if (is_numeric(type)) {
      return Key(getLongLong(column_index));
    } else {
      return Key(getText(column_index));
    }
  }
    
  bool isNull(int column_index) const override {
    if (!results_available_) {
      return true;
    } else {
      return stmt_ ? sqlite3_column_type(stmt_, column_index) == SQLITE_NULL : true;
    }
  }
  
  ColumnType getColumnType(int column_index) const override {
    switch (sqlite3_column_type(stmt_, column_index)) {
    case SQLITE_INTEGER: return ColumnType::INTEGER;
    case SQLITE_FLOAT: return ColumnType::FLOAT;
    case SQLITE_BLOB: return ColumnType::VARCHAR; // should be blob
    case SQLITE3_TEXT: return ColumnType::VARCHAR;
    case SQLITE_NULL:
    default:
      break;
    }
    return ColumnType::ANY;
    }

  int getNumFields() const override {
    return num_columns_;
  }

  long long getLastInsertId() const override {
    return sqlite3_last_insert_rowid(db_);
  }
  size_t getAffectedRows() const override {
    return sqlite3_changes(db_);
  }

  const std::string & getColumnName(int column_index) override {
    if (column_names_.empty()) {
      for (int i = 0; i < num_columns_; i++) {
	column_names_.push_back(sqlite3_column_name(stmt_, i));
      }
    }
    auto idx = static_cast<size_t>(column_index);
    return idx < column_names_.size() ? column_names_[idx] : null_string;
  }

protected:
  void step();
    
private:
  sqlite3 * db_;
  sqlite3_stmt * stmt_;
  int num_columns_;
  std::vector<std::string> column_names_;
  
  static inline std::string null_string;
};

SQLite::SQLite(const std::string & db_file, bool read_only) : db_file_(db_file), read_only_(read_only)
{
  open();
}

SQLite::SQLite(const SQLite & other) : db_file_(other.db_file_), read_only_(other.read_only_)
{
  open();
}

SQLite::SQLite(SQLite && other)
  : db_file_(std::move(other.db_file_)), db_(std::exchange(other.db_, nullptr)), read_only_(other.read_only_) { }

SQLite::~SQLite() {
  if (db_) {
    int r = sqlite3_close(db_);
    if (r) {
      // error
    }
  }
}

bool
SQLite::open() {
  int flags = 0;
  if (db_file_.empty()) {
    flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  } else if (read_only_) {
    flags = SQLITE_OPEN_READONLY;
  } else {
    flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
  }
  int r = sqlite3_open_v2(db_file_.c_str(), &db_, flags, 0);
  if (r) {
    db_ = 0;
    throw SQLException(SQLException::OPEN_FAILED, sqlite3_errmsg(db_));
  }

  if (db_) {
    sqlite3_busy_timeout(db_, 1000);
  }

  return true;
}

std::unique_ptr<sqldb::SQLStatement>
SQLite::prepare(std::string_view query) {
  if (!db_) {
    throw SQLException(SQLException::PREPARE_FAILED);
  }
  sqlite3_stmt * stmt = 0;
  int r = sqlite3_prepare_v2(db_, query.data(), query.size(), &stmt, 0);
  if (r != SQLITE_OK) {
    throw SQLException(SQLException::PREPARE_FAILED, sqlite3_errmsg(db_), std::string(query));
  }
  assert(stmt);  
  return std::make_unique<SQLiteStatement>(db_, stmt);
}

void
SQLiteStatement::step() {
  results_available_ = false;

  while ( 1 ) {
    int r = sqlite3_step(stmt_);
    switch (r) {
    case SQLITE_ROW:
      results_available_ = true;
      return;
      
    case SQLITE_DONE:
      return;

    case SQLITE_BUSY:
      // warning
      break;
      
    case SQLITE_ERROR: throw SQLException(SQLException::DATABASE_ERROR, sqlite3_errmsg(db_));
    case SQLITE_MISUSE: throw SQLException(SQLException::DATABASE_MISUSE, sqlite3_errmsg(db_));
    case SQLITE_CONSTRAINT: throw SQLException(SQLException::CONSTRAINT_VIOLATION, sqlite3_errmsg(db_));
    case SQLITE_MISMATCH: throw SQLException(SQLException::MISMATCH, sqlite3_errmsg(db_));
      
    default:
      // error
      assert(0);
      return;
    }
  }
  
  throw SQLException(SQLException::QUERY_TIMED_OUT);
}
