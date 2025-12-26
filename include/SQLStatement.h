#ifndef _SQLDB_SQLSTATEMENT_H_
#define _SQLDB_SQLSTATEMENT_H_

#include "DataStream.h"

namespace sqldb {
  class SQLStatement : public DataStream {
  public:
    SQLStatement() = default;

    void reset() override {
      DataStream::reset();
      results_available_ = false;
    }

    virtual size_t getAffectedRows() const = 0;
    virtual size_t getNumWarnings() const { return 0; }
  
    bool resultsAvailable() const noexcept { return results_available_; }

  protected:    
    bool results_available_ = false;
  };
};

#endif
