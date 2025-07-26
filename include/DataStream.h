#ifndef _SQLDB_DATASTREAM_H_
#define _SQLDB_DATASTREAM_H_

#include "ColumnType.h"
#include "Key.h"

#include <vector>
#include <string_view>

namespace sqldb {  
  class DataStream {
  public:
    DataStream() { }
    DataStream(const DataStream & other) = delete;
    DataStream(const DataStream && other) = delete;

    DataStream & operator=(const DataStream & other) = delete;
    DataStream & operator=(const DataStream && other) = delete;
    
    virtual ~DataStream() { }

    virtual size_t execute() = 0;

    virtual std::vector<uint8_t> getBlob(int column_index) = 0;
    virtual std::string_view getText(int column_index) = 0;
    virtual bool isNull(int column_index) const = 0;
    virtual int getNumFields() const = 0;
    virtual bool next() = 0;
    virtual const std::string & getColumnName(int column_index) = 0;

    virtual const std::vector<float> & getVector(int column_index) { return null_vector; }

    virtual ColumnType getColumnType(int column_index) const {
      return column_index >= 0 && column_index < getNumFields() ? ColumnType::TEXT : ColumnType::ANY;
    }
    
    virtual bool getBool(int column_index, bool default_value = false) {
      return getInteger(column_index, default_value ? 1 : 0) ? true : false;
    }

    virtual double getDouble(int column_index, double default_value = 0.0) = 0;
    virtual float getFloat(int column_index, float default_value = 0.0f) = 0;
    virtual long long getInteger(int column_index, long long default_value = 0) = 0;
    virtual Key getKey(int column_index) = 0;

    virtual void set(int column_idx, std::string_view value, bool is_defined = true) = 0;
    virtual void set(int column_idx, int value, bool is_defined = true) = 0;
    virtual void set(int column_idx, long long value, bool is_defined = true) = 0;
    virtual void set(int column_idx, double value, bool is_defined = true) = 0;
    virtual void set(int column_idx, const void * data, size_t len, bool is_defined) = 0;

    virtual void set(int column_idx, float value, bool is_defined = true) {
      set(column_idx, static_cast<double>(value), is_defined);
    }

    virtual void set(int column_idx, const sqldb::Key & key) {
      if (key.empty()) {
	set(column_idx, 0, false);
      } else if (key.size() >= 2) {
	set(column_idx, key.serializeToText());
      } else if (sqldb::is_numeric(key.getType(0))) {
	set(column_idx, key.getInteger(0));
      } else {
	set(column_idx, key.getText(0));
      }
    }

    virtual long long getLastInsertId() const = 0;

    void bind(int value, bool is_defined = true) {
      set(getNextBindIndex(), value, is_defined);
    }
    void bind(long long value, bool is_defined = true) {
      set(getNextBindIndex(), value, is_defined);
    }  
    void bind(double value, bool is_defined = true) {
      set(getNextBindIndex(), value, is_defined);
    }
    void bind(std::string_view value, bool is_defined = true) {
      set(getNextBindIndex(), std::move(value), is_defined);
    }
    void bind(const void * data, size_t len, bool is_defined) {
      set(getNextBindIndex(), data, len, is_defined);
    }
    void bind(const std::vector<uint8_t> & data, bool is_defined = true) {
      set(getNextBindIndex(), data.data(), data.size(), is_defined);
    }
    void bind(const Key & value) {
      set(getNextBindIndex(), value);
    }
    void bind(bool value, bool is_defined = true) {
      bind(value ? 1 : 0, is_defined);
    }

    virtual void reset() {
      next_bind_index_ = 0;
    }

  protected:
    int getNextBindIndex() { return next_bind_index_++; }

  private:
    int next_bind_index_ = 0;

    static inline std::vector<float> null_vector;
  };
};

#endif
