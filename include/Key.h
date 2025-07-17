#ifndef _SQLDB_KEY_H_
#define _SQLDB_KEY_H_

#include "ColumnType.h"

#include <string>
#include <vector>
#include <variant>

#include "robin_hood.h"

namespace sqldb {
  static inline size_t hash_combine(size_t seed, size_t v) noexcept {
    // see https://www.boost.org/doc/libs/1_55_0/doc/html/hash/reference.html#boost.hash_combine
    seed ^= v + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
  }
  
  class Key {
  public:
    Key() noexcept { }
    explicit Key(int value) noexcept {
      addComponent(value);
    }
    explicit Key(std::string value) noexcept {
      if (!value.empty()) addComponent(std::move(value));
    }
    explicit Key(std::string_view value) noexcept {
      addComponent(std::string(value));
    }
    explicit Key(const char * value) noexcept {
      addComponent(std::string(value));
    }
    explicit Key(int value1, const Key & value2) noexcept {
      addComponent(value1);
      startColumn();
      for (size_t i = 0; i < value2.size(); i++) {
	addComponent(value2, i);
      }
    }
    explicit Key(std::string value1, std::string value2) noexcept {
      addComponent(std::move(value1));
      addComponent(std::move(value2));
    }
    explicit Key(std::string value1, int value2) noexcept {
      addComponent(std::move(value1));
      addComponent(value2);
    }
    explicit Key(std::string_view value1, int value2) noexcept {
      addComponent(std::move(value1));
      addComponent(value2);
    }
    explicit Key(const char * value1, int value2) noexcept {
      addComponent(std::string(value1));
      addComponent(value2);
    }
    explicit Key(Key value1, Key value2, Key value3, Key value4) noexcept {
      addComponent(std::move(value1));
      startColumn();
      addComponent(std::move(value2));
      startColumn();
      addComponent(std::move(value3));
      startColumn();
      addComponent(std::move(value4));
    }
    explicit Key(int value1, int value2, int value3, int value4) noexcept {
      addComponent(value1);
      addComponent(value2);
      addComponent(value3);
      addComponent(value4);
    }
    explicit Key(int value1, int value2) noexcept {
      addComponent(value1);
      addComponent(value2);
    }
    explicit Key(std::initializer_list<std::string> v) {
      for (auto & a : v) {
	addComponent(a);
      }
    }

    bool operator < (const Key & other) const noexcept {
      for (size_t pos = 0; pos < components_.size() && pos < other.components_.size(); pos++) {
	if (components_[pos] < other.components_[pos]) return true;
	else if (components_[pos] > other.components_[pos]) return false;
      }
      if (components_.size() < other.components_.size()) return true;
      else return false;      
    }

    bool operator == (const Key & other) const noexcept {
      return components_ == other.components_;
    }
    bool operator != (const Key & other) const noexcept {
      return components_ != other.components_;
    }

    void clear() noexcept {
      components_.clear();
      columns_.clear();
    }
    
    bool empty() const noexcept { return components_.empty(); }
    size_t size() const noexcept { return components_.size(); }
    void resize(size_t s) { components_.resize(s); }

    // remove the first component
    void shift() { components_.erase(components_.begin()); }
    void unshift(long long value) { components_.insert(components_.begin(), value); }
    void pop_back() { components_.pop_back(); }

    void addComponent(int value) noexcept {
      addComponent(static_cast<long long>(value));
    }

    void addComponent(std::string_view value) noexcept {
      addComponent(std::string(value));
    }

    void addComponent(long long value) noexcept {
      components_.push_back(value);
      if (columns_.empty()) columns_.push_back(0);
      columns_.back()++;
    }

    void addComponent(std::string value) noexcept {
      components_.push_back(std::move(value));
      if (columns_.empty()) columns_.push_back(0);
      columns_.back()++;
    }

    void addComponent(const Key & other, size_t idx = 0) noexcept {
      if (is_numeric(other.getType(idx))) {
	addComponent(other.getLongLong(idx));
      } else {
	addComponent(other.getText(idx));
      }
    }

    void addSubKey(const Key & other) noexcept {
      for (size_t i = 0; i < other.size(); i++) {
	if (is_numeric(other.getType(i))) {
	  addComponent(other.getLongLong(i));
	} else {
	  addComponent(other.getText(i));
	}
      }
    }

    void startColumn() {
      columns_.push_back(0);
    }

    void setComponent(size_t idx, long long value) { components_[idx] = value; }
    void setComponent(size_t idx, std::string_view value) { components_[idx] = std::string(value); }

    void assignComponents(size_t pos, const Key & other) {
      for (size_t i = 0; i < other.size(); i++) {
	components_[pos + i] = other.components_[i];
      }
    }

    ColumnType getType(size_t idx) const noexcept {
      if (idx < components_.size()) {
	auto & component = components_[idx];
	if (std::holds_alternative<long long>(component)) {
	  return ColumnType::INT64;
	} else if (std::holds_alternative<std::string>(component)) {
	  return ColumnType::VARCHAR;
	}
      }
      return ColumnType::ANY;
    }
    
    int getInt(size_t idx) const noexcept {
      return getLongLong(idx);
    }
    
    long long getLongLong(size_t idx) const noexcept {
      if (idx < components_.size()) {
	if (std::holds_alternative<long long>(components_[idx])) {
	  return std::get<long long>(components_[idx]);
	} else {
	  return std::stoll(std::get<std::string>(components_[idx]));
	}
      }
      return 0LL;
    }

    const std::string & getText(size_t idx) const noexcept {
      if (idx < components_.size()) {
	if (std::holds_alternative<std::string>(components_[idx])) {
	  return std::get<std::string>(components_[idx]);
	}
      }
      return empty_string;
    }

    Key getSubKey(size_t from) const noexcept {
      Key key;
      for (size_t idx = from; idx < size(); idx++) {
	if (std::holds_alternative<std::string>(components_[idx])) {
	  key.addComponent(std::get<std::string>(components_[idx]));
	} else {
	  key.addComponent(std::get<long long>(components_[idx]));
	}
      }
      return key;
    }

    Key getSubKey(size_t from, size_t n) const noexcept {
      Key key;
      for (size_t idx = from; idx < size() && idx < from + n; idx++) {
	if (std::holds_alternative<std::string>(components_[idx])) {
	  key.addComponent(std::get<std::string>(components_[idx]));
	} else {
	  key.addComponent(std::get<long long>(components_[idx]));
	}
      }
      return key;
    }

    Key getColumn(size_t i) const noexcept {
      if (i < columns_.size()) {
	size_t start = 0;
	for (size_t j = 0; j < i; j++) start += columns_[j];
	return getSubKey(start, columns_[i]);
      } else {
	return sqldb::Key();
      }      
    }

    Key getParentKey() const noexcept {
      if (size() >= 2) {
	sqldb::Key key = *this;
	key.pop_back();
	return key;
      } else {
	return sqldb::Key();
      }
    }

    inline size_t getHash() const noexcept {
      size_t seed = 0;
      for (auto & c : components_) {
	if (std::holds_alternative<long long>(c)) {
	  seed = hash_combine(seed, robin_hood::hash<long long>{}(std::get<long long>(c)));
	} else if (std::holds_alternative<std::string>(c)) {
	  seed = hash_combine(seed, robin_hood::hash<std::string>{}(std::get<std::string>(c)));
	} else {
	  seed = hash_combine(seed, robin_hood::hash<long long>{}(0));
	}
      }
      return seed;
    }

    std::string serializeToText() const noexcept {
      std::string s;
      for (size_t i = 0; i < components_.size(); i++) {
	if (i != 0) s += "|";
	if (is_numeric(getType(i))) {
	  s += std::to_string(getLongLong(i));
	} else {
	  s += getText(i);
	}
      }
      return s;
    }

  private:    
    std::vector<std::variant<long long, std::string>> components_;
    std::vector<unsigned short> columns_;

    static inline std::string empty_string;
  };

  static inline std::string to_string(const Key & key) noexcept {
    std::string s;
    for (size_t i = 0; i < key.size(); i++) {
      if (!s.empty()) s += "|";
      if (is_numeric(key.getType(i))) {
	s += std::to_string(key.getLongLong(i));
      } else {
	s += key.getText(i);
      }
    }
    return s;
  }
};

namespace robin_hood {
  template<>
  struct hash<sqldb::Key> {
    size_t operator()(sqldb::Key const & key) const noexcept {
      return key.getHash();      
    }
  };
}

namespace std {
  template <>
  struct hash<sqldb::Key> {
    std::size_t operator()(const sqldb::Key & key) const noexcept {
      return key.getHash();      
    }
  };
}

#endif
