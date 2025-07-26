#ifndef _SQLDB_COLUMNTYPE_H_
#define _SQLDB_COLUMNTYPE_H_

namespace sqldb {
  enum class ColumnType {
    ANY = 0,
    INTEGER,
    CHAR,
    BOOL,
    VARCHAR, // single line of text
    TEXT, // multiple lines of text
    DATETIME,
    DATE,
    FLOAT,
    DOUBLE,
    URL,
    TEXT_KEY,
    BINARY_KEY,
    ENUM,
    BLOB,
    VECTOR
  };

  static inline bool is_numeric(ColumnType type) {
    switch (type) {
    case ColumnType::INTEGER:
    case ColumnType::BOOL:
    case ColumnType::DATETIME:
    case ColumnType::DATE:
    case ColumnType::FLOAT:
    case ColumnType::DOUBLE:
    case ColumnType::VECTOR:
      return true;
    case ColumnType::ANY:
    case ColumnType::CHAR:
    case ColumnType::VARCHAR:
    case ColumnType::TEXT:
    case ColumnType::URL:
    case ColumnType::TEXT_KEY:
    case ColumnType::BINARY_KEY:
    case ColumnType::ENUM:
    case ColumnType::BLOB:
      break;
    }
    return false;
  }
};

#endif

