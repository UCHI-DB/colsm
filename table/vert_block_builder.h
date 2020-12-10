//
// Created by harper on 12/9/20.
//

#ifndef LEVELDB_BLOCK_VERT_BUILDER_H
#define LEVELDB_BLOCK_VERT_BUILDER_H

#include <cstdint>
#include "format.h"
#include "vert_block.h"
#include <vector>

namespace leveldb {
namespace vert {

/**
 * Builder for building vertical blocks
 */
class VertBlockBuilder {
 public:
  explicit VertBlockBuilder(const Options* options);

  VertBlockBuilder(const VertBlockBuilder&) = delete;
  VertBlockBuilder& operator=(const VertBlockBuilder&) = delete;

  virtual ~VertBlockBuilder();

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const;

 private:
  uint32_t section_size_;

  VertBlockMeta meta_;
  std::vector<VertSection*> section_buffer_;

  uint64_t offset_;

  VertSection* current_section_;

  char* internal_buffer_;

  void DumpSection();
};


}
}
#endif  // LEVELDB_BLOCK_VERT_BUILDER_H
