//
// Created by harper on 6/11/21.
//

#ifndef LEVELDB_DATABLOCKBUILDER_H
#define LEVELDB_DATABLOCKBUILDER_H

#include <memory>
#include "table/block_builder.h"
#include "colsm/vblock/vert_block_builder.h"

namespace colsm {

/**
 * DataBlockBuilder is a wrapper allowing the builder to switch between
 * normal and vertical format builders
 */
class DataBlockBuilder {
 public:
  explicit DataBlockBuilder(const Options* options, bool usev) {
    if(usev) {
      delegate_ = std::unique_ptr<BlockBuilder>(new VertBlockBuilder(options));
    } else {
      delegate_ = std::unique_ptr<BlockBuilder>(new BlockBuilder(options));
    }
  }

  DataBlockBuilder(const DataBlockBuilder&) = delete;
  DataBlockBuilder& operator=(const DataBlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset() { delegate_->Reset(); }

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value) { delegate_->Add(key,value);}

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish() {return delegate_->Finish();}

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const {
    return delegate_->CurrentSizeEstimate();
  }

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return delegate_->empty(); }

 private:
  std::unique_ptr<BlockBuilder> delegate_;
};

}  // namespace colsm
#endif  // LEVELDB_DATABLOCKBUILDER_H
