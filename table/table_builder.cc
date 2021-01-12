// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"
#include <iostream>
#include <assert.h>
#include <fstream>
#include <strstream>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
void cPrintBuffer(const void* pBuff, unsigned int nLen)
{
    if (NULL == pBuff || 0 == nLen) {
        return;
    }

    const int nBytePerLine = 16;
    unsigned char* p = (unsigned char*)pBuff;
    char szHex[3*nBytePerLine+1] = {0};

    printf("-----------------begin-------------------\n");
    for (unsigned int i=0; i<nLen; ++i) {
        int idx = 3 * (i % nBytePerLine);
        if (0 == idx) {
            memset(szHex, 0, sizeof(szHex));
        }
        snprintf(&szHex[idx], 4, "%02x ", p[i]); // buff长度要多传入1个字节
        
        // 以16个字节为一行，进行打印
        if (0 == ((i+1) % nBytePerLine)) {
          printf("%s\n", szHex);
        }
    }

    // 打印最后一行未满16个字节的内容
    if (0 != (nLen % nBytePerLine)) {
        printf("%s\n", szHex);
    }
    printf("------------------end-------------------\n");
}
// using namespace adgMod;
namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        _bytes(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  uint64_t _bytes;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;
  std::vector<std::pair<Slice, Slice>> all_values;
  // std::vector<std::pair<uint32_t, uint32_t>> block_pos;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }

  // new learnedMod
  RMIConfig rmi_config;
  RMIConfig::StageConfig first, second;

  first.model_type = RMIConfig::StageConfig::LinearRegression;
  first.model_n = 1;

  second.model_n = 1000;
  second.model_type = RMIConfig::StageConfig::LinearRegression;
  rmi_config.stage_configs.push_back(first);
  rmi_config.stage_configs.push_back(second);

  LearnedMod = new LearnedRangeIndexSingleKey<uint64_t,float> (rmi_config);
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
  delete LearnedMod;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  r->all_values.push_back({key, value});
  // std::cout << __func__ << " r->_bytes: " << r->_bytes << std::endl;
  r->_bytes += key.size();
  r->_bytes += value.size();
  Slice nkey (key.data(),8);
  uint64_t lekey = 0;
  sscanf(nkey.data(), "%8lld", &lekey);
  // memcpy(&lekey, nkey.data(), nkey.size());
  // lekey = TableBuilder::Fixed64(lekey);
  // lekey = strtoull (nkey.data(), NULL, 0);
  // std::cout << __func__ << " Add nkey: " << nkey.ToStringHex() << std::endl;
  // std::cout << __func__ << " Add lekey: " << lekey << std::endl;
  // std::cout << __func__ << " Add block_num: " << r->_bytes/4096 << std::endl;
  LearnedMod->insert(lekey,r->_bytes);

  // if (r->num_entries > 0) {
  //   assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  // }

  // if (r->pending_index_entry) {
  //   assert(r->data_block.empty());
  //   r->options.comparator->FindShortestSeparator(&r->last_key, key);
  //   std::string handle_encoding;
  //   r->pending_handle.EncodeTo(&handle_encoding);
  //   r->index_block.Add(r->last_key, Slice(handle_encoding));
  //   r->pending_index_entry = false;
  // }

  // if (r->filter_block != nullptr) {
  //   r->filter_block->AddKey(key);
  // }

  // r->last_key.assign(key.data(), key.size());
  // r->num_entries++;
  // r->data_block.Add(key, value);

  // const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  // if (estimated_block_size >= r->options.block_size) {
  //   Flush();
  // }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteLearnBlock(BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  string param;
  LearnedMod->serialize(param);
  
  // std::cout << __func__ << " param size:" << param.length() << " ;param: " << param << std::endl

  Slice raw(param);
  // cPrintBuffer(LearnedMod->param, LearnedMod->lenth);
  // std::cout << __func__ << " param size:" << param.length() << std::endl;
  // << " ;param: " << param << std::endl;

  // TODO(postrelease): Support more compression options: zlib?

  // WriteRawBlock(block_contents, type, handle);
  Rep* r = rep_;
  handle->set_offset(r->offset);
  // std::cout << __func__ << " r->offset: " << r->offset << std::endl;
  handle->set_size(raw.size());
  r->status = r->file->Append(raw);
  // r->compressed_output.clear();
  r->offset += raw.size();
  // block->Reset();
}


void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;

  LearnedMod->finish_insert();
  LearnedMod->finish_train();
  r->_bytes = 0;


  // Write data block
  int based = 0;
  if(ok()) {
    for(auto& item: r->all_values){
      Slice nkey (item.first.data(),8);
      uint64_t lekey = 0;
      sscanf(nkey.data(), "%8lld", &lekey);
      // memcpy(&lekey, nkey.data(), nkey.size());
      // lekey = TableBuilder::Fixed64(lekey);
      // lekey = strtoull (nkey.data(), NULL, 0);
      auto value_get = LearnedMod->get(lekey);
      // std::cout << __func__ << " value_get: " << value_get << std::endl;
      int block_num = value_get / 4096;
      // std::cout << __func__ << " write nkey: " << nkey.ToStringHex() << std::endl;
      // std::cout << __func__ <<" write lekey: " << lekey << std::endl;
      // std::cout << __func__ << " block_num: " << block_num << std::endl;
      // std::ofstream output_file("input.txt");
      // output_file.precision(15);
      // output_file << lekey << " " << block_num <<  "\n";

      if (r->num_entries > 0) {
        assert(r->options.comparator->Compare(item.first, Slice(r->last_key)) > 0);
      }

      if (r->pending_index_entry) {
        assert(r->data_block.empty());
        r->options.comparator->FindShortestSeparator(&r->last_key, item.first);
        std::string handle_encoding;
        r->pending_handle.EncodeTo(&handle_encoding);
        r->index_block.Add(r->last_key, Slice(handle_encoding));
        
        // output_file << lekey << " " << block_num <<  "\n";
        std::cout << __func__ << " Add: " << r->pending_handle.offset() << " ;key: " << r->last_key << std::endl;
        // r->block_pos.push_back({r->pending_handle.offset,r->pending_handle.size});
        r->pending_index_entry = false;
      }

      if (r->filter_block != nullptr) {
        r->filter_block->AddKey(item.first);
      }

      r->last_key.assign(item.first.data(), item.first.size());
      r->num_entries++;
      r->data_block.Add(item.first, item.second);

      // const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
      // if (estimated_block_size >= r->options.block_size) {
      //   Flush();
      // }
      if(block_num != based){
        Flush();
        based += 1;
      }
    }
    r->all_values.clear();
  }
  // std::cout << __func__ << " Add over" << std::endl;

  Flush();
  assert(!r->closed);
  r->closed = true;
  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle, learned_block_handle;

  // Write filter block
  // std::cout << __func__ << " Write filter block" << std::endl;
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // std::cout << __func__ << " Write metaindex block" << std::endl;
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

//#ifdef LEARNED_INDEX
  // if (ok()) {
  //   LearnedMod->FileLearn();
  //   //LeanredMod->BytesTrain();
  // }
//#endif

  // Write index block
  // std::cout << __func__ << " Write index block" << std::endl;
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      // std::cout << __func__ << " pending_handle: " << r->pending_handle.offset() << " ;pending_handle: " << r->pending_handle.size() << std::endl;
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
    // std::cout << __func__ << " :index_block_handle offset: " << index_block_handle.offset() << std::endl;
    // std::cout << __func__ << " :index_block_handle size: " << index_block_handle.size() << std::endl;
  }

  //write learned index into block
  if (ok()) {
    // std::cout << __func__ << " param: " << LearnedMod->param << std::endl;
    // LearnedMod->FileLearn();
    WriteLearnBlock(&learned_block_handle);
    // std::cout << __func__ << " :WriteLearnBlock over: " << learned_block_handle.offset() << std::endl;
    // std::cout << __func__ << " :size: " << learned_block_handle.size() << std::endl;
  } 


  // Write footer
  // std::cout << __func__ << " Write footer" << std::endl;
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    footer.set_learned_handle(learned_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  // std::cout << __func__ << " end!" << std::endl;
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
