#include <MySQL.h>

#include <cassert>
#include <cstring>

#include <SQLException.h>

#define MYSQL_MAX_BOUND_VARIABLES 255
#define MYSQL_BIND_BUFFER_SIZE 4096

using namespace std;
using namespace sqldb;

class MySQLStatement : public SQLStatement {
public:
  MySQLStatement(MYSQL_STMT * stmt) : stmt_(stmt) {
    assert(stmt_);
    num_bound_variables_ = mysql_stmt_param_count(stmt_);
        
    reset();
  }

  ~MySQLStatement() {
    if (stmt_) {
      mysql_stmt_free_result(stmt_);
      mysql_stmt_close(stmt_);
    }
  }
  
  size_t execute() override;
  void reset() override;
  bool next() override;
  
  void set(int column_idx, int value, bool is_defined = true) override;
  void set(int column_idx, long long value, bool is_defined = true) override;
  void set(int column_idx, std::string_view value, bool is_defined = true) override;
  void set(int column_idx, const void * data, size_t len, bool is_defined = true) override;
  void set(int column_idx, double value, bool is_defined = true) override;
  
  double getDouble(int column_idx, double default_value = 0.0) override;
  long long getInteger(int column_idx, long long default_value = 0LL) override;
  std::string_view getText(int column_idx) override;
  std::vector<uint8_t> getBlob(int column_idx) override;
  
  bool isNull(int column_idx) const override;
  
  long long getLastInsertId() const override { return last_insert_id_; }
  size_t getAffectedRows() const override { return rows_affected_; }
  size_t getNumWarnings() const override { return num_warnings_; }
  int getNumFields() const override { return num_bound_variables_; }

  const std::string & getColumnName(int column_idx) {
    // TODO
    return empty_string;
  }
    
protected:
  void setData(int column_idx, enum_field_types buffer_type, const void * ptr, size_t size, bool is_defined = true, bool is_unsigned = false);
  
private:
  MYSQL_STMT * stmt_;
  int num_bound_variables_ = 0;       
  bool has_result_set_ = false, is_query_executed_ = false;
  long long last_insert_id_ = 0;
  size_t rows_affected_ = 0, num_warnings_ = 0;
  
  MYSQL_BIND bind_data_[MYSQL_MAX_BOUND_VARIABLES];
  size_t bind_length_[MYSQL_MAX_BOUND_VARIABLES];
  my_bool bind_is_null_[MYSQL_MAX_BOUND_VARIABLES];
  my_bool bind_error_[MYSQL_MAX_BOUND_VARIABLES];
  char bind_out_buffer_[MYSQL_MAX_BOUND_VARIABLES * MYSQL_BIND_BUFFER_SIZE];
  // char bind_in_buffer_[MYSQL_MAX_BOUND_VARIABLES * MYSQL_BIND_BUFFER_SIZE];
  std::vector<unique_ptr<char[]>> bind_out_ptr_, bind_in_ptr_;

  static inline my_bool is_null = 1, is_not_null = 0;
  static inline std::string empty_string;
};

MySQL::~MySQL() {
  if (conn_) mysql_close(conn_);
}

void
MySQL::begin() {
  mysql_autocommit(conn_, 0); // disable autocommit
}

void
MySQL::commit() {
  if (mysql_commit(conn_) != 0) {
    mysql_autocommit(conn_, 1); // enable autocommit
    throw SQLException(SQLException::COMMIT_FAILED);
  } else {
    mysql_autocommit(conn_, 1); // enable autocommit
  }
}

void
MySQL::rollback() {
  if (mysql_rollback(conn_) != 0) {
    mysql_autocommit(conn_, 1); // enable autocommit
    throw SQLException(SQLException::ROLLBACK_FAILED);
  } else {
    mysql_autocommit(conn_, 1); // enable autocommit
  }
}

std::unique_ptr<SQLStatement>
MySQL::prepare(std::string_view query) {
  if (!conn_) {
    throw SQLException(SQLException::PREPARE_FAILED, "Not connected", std::string(query));
  }
  MYSQL_STMT * stmt = 0;
  while ( 1 ) {
    if (stmt) mysql_stmt_close(stmt);	
    stmt = mysql_stmt_init(conn_);
    if (!stmt) {
      if (mysql_errno(conn_) == 2006) {
	continue;
      }
      throw SQLException(SQLException::PREPARE_FAILED, mysql_error(conn_), std::string(query));
    }
    if (mysql_stmt_prepare(stmt, query.data(), query.size()) != 0) {
      if (mysql_errno(conn_) == 2006) {
	continue;
      }
      throw SQLException(SQLException::PREPARE_FAILED, mysql_error(conn_), std::string(query));
    }
    break;
  }
  return std::make_unique<MySQLStatement>(stmt);
}

void
MySQL::connect(string host_name, int port, string user_name, string password, string db_name) {
  host_name_ = std::move(host_name);
  port_ = port;
  user_name_ = std::move(user_name);
  password_ = std::move(password);
  db_name_ = std::move(db_name);
  
  connect();
}

void
MySQL::connect() {
  if (conn_) mysql_close(conn_);
  conn_ = mysql_init(NULL);
  if (!conn_) {
    throw SQLException(SQLException::INIT_FAILED);
  }

  int flags = CLIENT_FOUND_ROWS; 

  if (!mysql_real_connect(conn_, host_name_.c_str(), user_name_.c_str(), password_.c_str(), db_name_.c_str(), port_, 0, flags)) {
    auto errmsg = mysql_error(conn_);
    mysql_close(conn_);
    conn_ = 0;
    throw SQLException(SQLException::CONNECTION_FAILED, errmsg);
  }
  
  execute("SET NAMES utf8mb4");
}

bool
MySQL::ping() {
  if (conn_ && mysql_ping(conn_) == 0) {
    return true;
  } else {
    return false;
  }
}

std::pair<size_t, size_t>
MySQL::execute(std::string_view query) {
  if (mysql_real_query(conn_, query.data(), query.size()) != 0) {
    throw SQLException(SQLException::EXECUTE_FAILED, mysql_error(conn_), string(query));
  }
  auto affected_rows = mysql_affected_rows(conn_);
  auto warnings = mysql_warning_count(conn_);
  return pair(affected_rows, warnings);
}

size_t
MySQLStatement::execute() {
  is_query_executed_ = true;
  has_result_set_ = false;
  
  if (mysql_stmt_bind_param(stmt_, bind_data_) != 0) {
    throw SQLException(SQLException::EXECUTE_FAILED, mysql_stmt_error(stmt_));
  }
  
  // Fetch result set meta information
  auto prepare_meta_result = mysql_stmt_result_metadata(stmt_);
  
  if (mysql_stmt_execute(stmt_) != 0) {
    throw SQLException(SQLException::EXECUTE_FAILED, mysql_stmt_error(stmt_));
  }

  int a = mysql_stmt_affected_rows(stmt_);
  if (a > 0) {
    rows_affected_ = a;
  } else {
    rows_affected_ = 0;
  }
  num_warnings_ = mysql_stmt_warning_count(stmt_);
  last_insert_id_ = mysql_stmt_insert_id(stmt_);
    
  // NULL if no metadata / results
  if (prepare_meta_result) {
    // Get total columns in the query
    num_bound_variables_ = mysql_num_fields(prepare_meta_result);
    mysql_free_result(prepare_meta_result);

    // reserve varibles if larger than MAX
    memset(bind_data_, 0, num_bound_variables_ * sizeof(MYSQL_BIND));
    
    for (int i = 0; i < num_bound_variables_; i++) {
      bind_length_[i] = 0;
      
      // bind[i].buffer_type = MYSQL_TYPE_STRING;
      bind_data_[i].buffer = 0; // new char[255];
      bind_data_[i].buffer_length = 0; // 255;
      bind_data_[i].is_null = &bind_is_null_[i];
      bind_data_[i].length = &bind_length_[i];
      bind_data_[i].error = &bind_error_[i];
    }
    
    /* Bind the result buffers */
    if (mysql_stmt_bind_result(stmt_, bind_data_) || mysql_stmt_store_result(stmt_)) {
      throw SQLException(SQLException::EXECUTE_FAILED, mysql_stmt_error(stmt_));
    }
    
    has_result_set_ = true;
  }

  return rows_affected_;
}

void
MySQLStatement::reset() {
  SQLStatement::reset();

  rows_affected_ = 0;
  is_query_executed_ = false;
  has_result_set_ = false;

  bind_out_ptr_.clear();
  bind_in_ptr_.clear();

  memset(bind_data_, 0, num_bound_variables_ * sizeof(MYSQL_BIND));
  for (int i = 0; i < num_bound_variables_; i++) {
    set(i, 0, false);
  } 
  // no need to reset. just rebind parameter and execute again. not tested though
}

bool
MySQLStatement::next() {
  SQLStatement::reset();
  
  assert(stmt_);

  bind_in_ptr_.clear();
  rows_affected_ = 0;
  
  if (!is_query_executed_) {
    execute();
  }
  
  if (has_result_set_) {
    int r = mysql_stmt_fetch(stmt_);
        
    if (r == 0) {
      results_available_ = true;
    } else if (r == MYSQL_NO_DATA) {
    } else if (r == MYSQL_DATA_TRUNCATED) {
      results_available_ = true;
    } else if (r) {
      throw SQLException(SQLException::EXECUTE_FAILED, mysql_stmt_error(stmt_));
    }
  }
  
  return results_available_;
}

#if 0
MySQLStatement &
MySQLStatement::bindNull() {
  int a = 0;
  return bindData(MYSQL_TYPE_LONG, &a, sizeof(a), false);
}
#endif

void
MySQLStatement::set(int column_idx, int value, bool is_defined) {
  if (!is_defined) value = 0;
  setData(column_idx, MYSQL_TYPE_LONG, &value, sizeof(value), is_defined); 
}

void
MySQLStatement::set(int column_idx, long long value, bool is_defined) {
  if (!is_defined) value = 0;
  setData(column_idx, MYSQL_TYPE_LONGLONG, &value, sizeof(value), is_defined);
}

void
MySQLStatement::set(int column_idx, double value, bool is_defined) {
  if (!is_defined) value = 0.0;
  setData(column_idx, MYSQL_TYPE_DOUBLE, &value, sizeof(value), is_defined);
}

void
MySQLStatement::set(int column_idx, std::string_view value, bool is_defined) {
  setData(column_idx, MYSQL_TYPE_STRING, value.data(), value.size(), is_defined);
}

void
MySQLStatement::set(int column_idx, const void * data, size_t len, bool is_defined) {
  setData(column_idx, MYSQL_TYPE_BLOB, data, len, is_defined);
}

double
MySQLStatement::getDouble(int column_idx, double default_value) {
  if (column_idx < 0 || column_idx >= MYSQL_MAX_BOUND_VARIABLES) throw SQLException(SQLException::BAD_COLUMN_INDEX, "");
  assert(stmt_);
  double a = default_value;
   
  if (bind_is_null_[column_idx]) {
    // cerr << "null value\n";
  } else if (1 || bind_length_[column_idx]) {
    long unsigned int dummy1;
    my_bool dummy2;
    MYSQL_BIND b;
    memset(&b, 0, sizeof(MYSQL_BIND));
    b.buffer_type = MYSQL_TYPE_DOUBLE;
    b.buffer = (char *)&a;  
    b.buffer_length = sizeof(a);
    b.length = &dummy1; 
    b.is_null = &dummy2;
    if (mysql_stmt_fetch_column(stmt_, &b, column_idx, 0) != 0) {
      throw SQLException(SQLException::GET_FAILED, mysql_stmt_error(stmt_));
    }
  } else {
    // cerr << "empty value\n";
  }

  return a;
}

long long
MySQLStatement::getInteger(int column_idx, long long default_value) {
  if (column_idx < 0 || column_idx >= MYSQL_MAX_BOUND_VARIABLES) throw SQLException(SQLException::BAD_COLUMN_INDEX, "");

  assert(stmt_);
  
  long long a = default_value;
   
  if (bind_is_null_[column_idx]) {

  } else if (bind_length_[column_idx]) {
    long unsigned int dummy1;
    my_bool dummy2;
    MYSQL_BIND b;
    memset(&b, 0, sizeof(MYSQL_BIND));
    b.buffer_type = MYSQL_TYPE_LONGLONG;
    b.buffer = (char *)&a;  
    b.buffer_length = sizeof(a);
    b.length = &dummy1;
    b.is_null = &dummy2;
    if (mysql_stmt_fetch_column(stmt_, &b, column_idx, 0) != 0) {
      throw SQLException(SQLException::GET_FAILED, mysql_stmt_error(stmt_));
    }
  }

  return a;
}

string_view
MySQLStatement::getText(int column_idx) {
  if (column_idx < 0 || column_idx >= MYSQL_MAX_BOUND_VARIABLES) throw SQLException(SQLException::BAD_COLUMN_INDEX, "");

  assert(stmt_);
    
  if (!bind_is_null_[column_idx]) {
    auto len = bind_length_[column_idx];

    if (len) {
      auto tmp = make_unique<char[]>(len);
	
      long unsigned int dummy1;
      my_bool dummy2;
      MYSQL_BIND b;
      memset(&b, 0, sizeof(MYSQL_BIND));
      b.buffer_type = MYSQL_TYPE_STRING;
      b.buffer = tmp.get();
      b.buffer_length = len;
      b.length = &dummy1; 
      b.is_null = &dummy2;
      
      if (mysql_stmt_fetch_column(stmt_, &b, column_idx, 0) != 0) {
	throw SQLException(SQLException::GET_FAILED, mysql_stmt_error(stmt_));
      }

      string_view r(tmp.get(), len);
      
      if (static_cast<int>(bind_in_ptr_.size()) <= column_idx) bind_in_ptr_.resize(column_idx + 1);
      bind_in_ptr_[column_idx] = std::move(tmp);

      return r;
    }
  }
  
  return empty_string;
}

std::vector<uint8_t>
MySQLStatement::getBlob(int column_idx) {
  if (column_idx < 0 || column_idx >= MYSQL_MAX_BOUND_VARIABLES) throw SQLException(SQLException::BAD_COLUMN_INDEX, "");

  assert(stmt_);
  
  std::vector<uint8_t> s;
  
  if (!bind_is_null_[column_idx]) {
    auto len = (size_t)bind_length_[column_idx];
    if (len) {
      auto tmp = make_unique<char[]>(len);
      
      long unsigned int dummy1;
      my_bool dummy2;
      MYSQL_BIND b;
      
      memset(&b, 0, sizeof(MYSQL_BIND));
      b.buffer_type = MYSQL_TYPE_BLOB;
      b.buffer = tmp.get();
      b.buffer_length = len;
      b.length = &dummy1; 
      b.is_null = &dummy2;
      
      if (mysql_stmt_fetch_column(stmt_, &b, column_idx, 0) != 0) {
	throw SQLException(SQLException::GET_FAILED, mysql_stmt_error(stmt_));
      }
      for (size_t i = 0; i < len; i++) {
	s.push_back(tmp[i]);
      }
    }
  }

  return s;
}

bool
MySQLStatement::isNull(int column_idx) const {
  if (column_idx < 0 || column_idx >= MYSQL_MAX_BOUND_VARIABLES) throw SQLException(SQLException::BAD_COLUMN_INDEX, "");
  return bind_is_null_[column_idx];
}

void
MySQLStatement::setData(int column_idx, enum_field_types buffer_type, const void * ptr, size_t size, bool is_defined, bool is_unsigned) {
  if (column_idx < 0 || column_idx >= num_bound_variables_) {
    throw SQLException(SQLException::BAD_COLUMN_INDEX, "");
  }
  char * buffer;
  if (size <= MYSQL_BIND_BUFFER_SIZE) {
    buffer = &bind_out_buffer_[column_idx * MYSQL_BIND_BUFFER_SIZE];
  } else {
    auto tmp = unique_ptr<char[]>(new char[size]);
    buffer = tmp.get();
    if (static_cast<int>(bind_out_ptr_.size()) <= column_idx) bind_out_ptr_.resize(column_idx + 1);
    bind_out_ptr_[column_idx] = move(tmp);
  }
  memcpy(buffer, ptr, size);
  bind_data_[column_idx].buffer_type = buffer_type;
  bind_data_[column_idx].buffer = buffer;
  bind_data_[column_idx].buffer_length = size;
  bind_data_[column_idx].is_unsigned = is_unsigned;
  if (is_defined) {
    bind_data_[column_idx].is_null = &is_not_null;
  } else {
    bind_data_[column_idx].is_null = &is_null;
  }
}
