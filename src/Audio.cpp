#include "Audio.h"

#include <Cursor.h>

#include <sndfile.h>

#include <fmt/core.h>

#include <vector>
#include <cassert>

using namespace sqldb;
using namespace std;

class sqldb::AudioFile {
public:
  AudioFile(std::string filename) : filename_(move(filename)) {
    open();    
  }
  
  AudioFile(const AudioFile & other) : filename_(other.filename_) {
    open();
  }

  ~AudioFile() {
    if (f_) sf_close(f_);
  }

  std::vector<float> read(size_t offset, size_t frames) {
    sf_seek(f_, offset * num_channels_, SEEK_SET);
    sf_count_t mc_frames = frames * num_channels_;

    vector<float> buffer;
    int readcount;
    unique_ptr<float[]> buf(new float[4096]);
    while ((readcount = (int) sf_readf_float(f_, buf.get(), 4096)) > 0) {
      for (int k = 0 ; k < readcount; k++) {
	buffer.push_back(buf[k]);
      }
    }
    
    return buffer;
  }

  int getNumFields() const { return 4; }

  const std::string & getColumnName(int column_index) const {
    switch (column_index) {
    case 0: return name0;
    case 1: return name1;
    case 2: return name2;
    case 3: return name3;
    }
    return empty_string;
  }

  ColumnType getColumnType(int column_index) const {
    switch (column_index) {
    case 0: return ColumnType::VARCHAR;
    case 1: return ColumnType::VECTOR;
    case 2: case 3: return ColumnType::INTEGER;
    }
    return ColumnType::ANY;
  }

  long long getNumFrames() const { return num_frames_; }

  std::string_view getText(int column_index) const {
    switch (column_index) {
    case 2: return num_channels_str_;
    case 3: return sample_rate_str_;
    }
    return empty_string;
  }

  int getInt(int column_index, int default_value = 0) const {
    switch (column_index) {
    case 2: return num_channels_;
    case 3: return sample_rate_;
    }
    return default_value;
  }

protected:
  void open() {
    SF_INFO sfinfo;
    memset(&sfinfo, 0, sizeof(sfinfo));

    if ((f_ = sf_open(filename_.c_str(), SFM_READ, &sfinfo)) != nullptr) {
      num_channels_ = sfinfo.channels;
      sample_rate_ = sfinfo.samplerate;
      num_frames_ = sfinfo.frames / sfinfo.channels;
    } else {
      throw std::runtime_error(sf_strerror(nullptr));
    }
    num_channels_str_ = std::to_string(num_channels_);    
    sample_rate_str_ = std::to_string(sample_rate_);
  }
 
private:    
  std::string filename_;
  SNDFILE * f_;
  long long num_frames_ = 0;
  int num_channels_ = 0, sample_rate_ = 0;
  std::string num_channels_str_, sample_rate_str_;

  static inline std::string name0 = "Title";
  static inline std::string name1 = "Audio";
  static inline std::string name2 = "Channels";
  static inline std::string name3 = "Sample Rate";
  static inline std::string empty_string;
};

class AudioCursor : public Cursor {
public:
  AudioCursor(const std::shared_ptr<sqldb::AudioFile> & audio, int track, long long from, long long to)
    : audio_(audio), track_(track), from_(from), to_(to) {
    updateRowKey();
  }
  
  bool next() override {
    has_data_ = false;
    return false;
  }
  
  std::string_view getText(int column_index) override {
    return audio_->getText(column_index);
  }

  double getDouble(int column_index, double default_value = 0.0) override {
    return audio_->getInt(column_index, static_cast<int>(default_value));
  }

  float getFloat(int column_index, float default_value = 0.0f) override {
    return audio_->getInt(column_index, static_cast<int>(default_value));
  }

  long long getInteger(int column_index, long long default_value = 0) override {
    return audio_->getInt(column_index, default_value);    
  }

  Key getKey(int column_index) override {
    return Key(getText(column_index));
  }

  int getNumFields() const override { return audio_->getNumFields(); }

  vector<uint8_t> getBlob(int column_index) override {
    return std::vector<uint8_t>();
  }

  const std::vector<float> & getVector(int column_index) override {
    if (column_index == 1) {
      if (!has_data_) {
	has_data_ = true;
	if (!from_ && !to_) {
	  data_ = audio_->read(0, audio_->getNumFrames());
	} else {
	  data_ = audio_->read(from_, to_ - from_);
	}
      }
      return data_;
    } else {
      return DataStream::getVector(column_index); // return empty vector
    }
  }

  bool isNull(int column_index) const override { return false; }
  
  void set(int column_idx, std::string_view value, bool is_defined = true) override {
    throw std::runtime_error("Audio is read-only");
  }
  void set(int column_idx, int value, bool is_defined = true) override {
    throw std::runtime_error("Audio is read-only");
  }
  void set(int column_idx, long long value, bool is_defined = true) override {
    throw std::runtime_error("Audio is read-only");
  }
  void set(int column_idx, double value, bool is_defined = true) override {
    throw std::runtime_error("Audio is read-only");
  }
  void set(int column_idx, const void * data, size_t len, bool is_defined = true) override {
    throw std::runtime_error("Audio is read-only");
  }
  size_t execute() override {
    throw std::runtime_error("Audio is read-only");
  }
  size_t update(const Key & key) override {
    throw std::runtime_error("Audio is read-only");
  }

  long long getLastInsertId() const override { return 0; }

  const std::string & getColumnName(int column_index) override {
    return audio_->getColumnName(column_index);
  }

  ColumnType getColumnType(int column_index) const override {
    return audio_->getColumnType(column_index);
  }

  bool isColumnNullable(int col) const override {
    return false;
  }

  bool isColumnUnique(int col) const override {
    return false;
  }

protected:
  void updateRowKey() {
    Key key;
    key.addComponent(track_);
    if (from_ || to_) {
      key.addComponent(from_);
      key.addComponent(to_);
    }
    setRowKey(std::move(key));
  }

private:
  std::shared_ptr<AudioFile> audio_;
  int track_;
  long long from_, to_;
  bool has_data_ = false;
  std::vector<float> data_;
};

Audio::Audio(std::string filename) {
  audio_.push_back(make_shared<AudioFile>(move(filename)));

  std::vector<ColumnType> key_type = { ColumnType::INTEGER };
  setKeyType(std::move(key_type));
  setHasHumanReadableKey(true);
}

Audio::Audio(const Audio & other)
  : Table(other)
{
  for (auto & a : other.audio_) {
    audio_.push_back(make_shared<AudioFile>(*a));
  }
}

Audio::Audio(Audio && other)
  : Table(other),
    audio_(move(other.audio_)) { }

int
Audio::getNumFields(int track) const {
  return audio_[track]->getNumFields();
}

const std::string &
Audio::getColumnName(int column_index, int track) const {
  return audio_[track]->getColumnName(column_index);
}

ColumnType
Audio::getColumnType(int column_index, int track) const {
  return audio_[track]->getColumnType(column_index);
}

std::unique_ptr<Cursor>
Audio::seekBegin(int track) {
  return make_unique<AudioCursor>(audio_[track], track, 0, 0);
}

unique_ptr<Cursor>
Audio::seek(const Key & key) {
  int track = key.getInteger(0);
  if (key.size() == 1) {
    return make_unique<AudioCursor>(audio_[track], track, 0, 0);
  } else {
    return make_unique<AudioCursor>(audio_[track], track, key.getInteger(1), key.getInteger(2));
  }
}
