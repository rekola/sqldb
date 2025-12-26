#ifndef _LOG_H_
#define _LOG_H_

#include "Key.h"

#include <vector>
#include <mutex>

namespace sqldb {
  class Log {
  public:
    enum class Event { ADD = 1, REMOVE };
    
    Log() { }

    // Table log must be shared between copies
    Log(const Log & other) = delete;
    Log(Log && other) = delete;
    Log & operator=(const Log & other) = delete;
    Log & operator=(Log && other) = delete;

    void add(sqldb::Key key) {
      std::lock_guard<std::mutex> guard(mutex_);
      data_.emplace_back(Event::ADD, std::move(key));
    }
    void remove(sqldb::Key key) {
      std::lock_guard<std::mutex> guard(mutex_);
      data_.emplace_back(Event::REMOVE, std::move(key));
    }
    size_t size() const noexcept {
      std::lock_guard<std::mutex> guard(mutex_);
      return data_.size();
    }

    std::pair<Event, sqldb::Key> getEvent(int row) const {
      std::lock_guard<std::mutex> guard(mutex_);
      if (row >= 0 && row < static_cast<int>(data_.size())) {
	return data_[row];
      }
      return std::pair<Event, sqldb::Key>();
    }

    std::vector<std::pair<Event, sqldb::Key>> getEvents(size_t cursor) const {
      std::lock_guard<std::mutex> guard(mutex_);
      return std::vector<std::pair<Event, sqldb::Key>>(data_.begin() + cursor, data_.end());
    }

    void append(const Log & other) {
      auto other_data = other.getData();
      
      std::lock_guard<std::mutex> guard(mutex_);
      data_.insert(data_.end(), other_data.begin(), other_data.end());
    }

  protected:
    std::vector<std::pair<Event, sqldb::Key>> getData() const {
      std::vector<std::pair<Event, sqldb::Key>> r;
      {
	std::lock_guard<std::mutex> guard(mutex_);
	r = data_;
      }
      return r;
    }
    
  private:
    std::vector<std::pair<Event, sqldb::Key>> data_;
    mutable std::mutex mutex_;
  };
};

#endif
