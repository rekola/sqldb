#ifndef _SQLDB_AUDIO_H_
#define _SQLDB_AUDIO_H_

#include "Table.h"
#include "Cursor.h"

#include <stdexcept>

namespace sqldb {
  class AudioFile;
  
  class Audio : public Table {
  public:
    Audio(std::string filename);
    Audio(const Audio & other);
    Audio(Audio && other);

    std::unique_ptr<Table> copy() const override { return std::make_unique<Audio>(*this); }
       
    int getNumFields(int sheet) const override;    
    const std::string & getColumnName(int column_index, int sheet) const override;
    ColumnType getColumnType(int column_index, int sheet) const override;

    bool isColumnNullable(int column_index, int sheet = 0) const override { return true; }
    bool isColumnUnique(int column_index, int sheet = 0) const override { return false; }

    void clear() override {
      throw std::runtime_error("Audio is read-only");
    }

    void addColumn(std::string_view name, sqldb::ColumnType type, bool nullable, bool unique, int decimals) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> insert(const Key & key) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> insert(int sheet) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> increment(const Key & key) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> assign(std::vector<int> columns) override {
      throw std::runtime_error("Audio is read-only");
    }
    
    void remove(const Key & key) override {
      throw std::runtime_error("Audio is read-only");
    }

    std::unique_ptr<Cursor> seekBegin(int sheet) override;
    std::unique_ptr<Cursor> seek(const Key & key) override;
    
  private:
    // audio_ is shared with cursors
    std::vector<std::shared_ptr<AudioFile>> audio_;
  };
};

#endif
