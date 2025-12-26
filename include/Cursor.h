#ifndef _SQLDB_CURSOR_H_
#define _SQLDB_CURSOR_H_

#include "DataStream.h"

namespace sqldb {
  class Cursor : public DataStream {
  public:
    Cursor() { }

    virtual size_t update(const Key & key) = 0;

    const Key & getRowKey() const noexcept { return row_key_; }

  protected:
    void setRowKey(sqldb::Key key) { row_key_ = std::move(key); }
		   
    sqldb::Key row_key_;
  };
};

#endif
