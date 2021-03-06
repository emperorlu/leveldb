// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"
#include <sstream>
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include <vector>
#include <cstdint>
#include <cassert>
#include <utility>
#include <cmath>
#include <iostream>
#include <fstream>

// void PrintBuffer(const void* pBuff, unsigned int nLen)
// {
//     if (NULL == pBuff || 0 == nLen) {
//         return;
//     }

//     const int nBytePerLine = 16;
//     unsigned char* p = (unsigned char*)pBuff;
//     char szHex[3*nBytePerLine+1] = {0};

//     printf("-----------------begin-------------------\n");
//     for (unsigned int i=0; i<nLen; ++i) {
//         int idx = 3 * (i % nBytePerLine);
//         if (0 == idx) {
//             memset(szHex, 0, sizeof(szHex));
//         }
//         snprintf(&szHex[idx], 4, "%02x ", p[i]); // buff长度要多传入1个字节
        
//         // 以16个字节为一行，进行打印
//         if (0 == ((i+1) % nBytePerLine)) {
//           printf("%s\n", szHex);
//         }
//     }

//     // 打印最后一行未满16个字节的内容
//     if (0 != (nLen % nBytePerLine)) {
//         printf("%s\n", szHex);
//     }
//     printf("------------------end-------------------\n");
// }

namespace leveldb {
// struct Mod{
//   int max_lenth;
//   int str_size;
//   std::vector<char> based_char;
//   std::vector<double>based_num;
//   // std::vector<Segment> string_segments;

// };
struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
    delete learnedMod;
  }

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;
  FilterBlockReader* filter;
  const char* filter_data;
  // Mod* learnedMod;
  LearnedRangeIndexSingleKey<uint64_t,float>* learnedMod;
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
  std::vector<std::pair<uint32_t, uint32_t>> block_pos;
};

Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }

  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // BlockContents learn_block_contents;
  // Mod* learnmod = new Mod;
  // if (s.ok()) {
  //   // std::cout << __func__ << " n=" << std::endl;

  //   // ReadOptions opt;
  //   // s = ReadBlock(file, opt, footer.learned_handle(), &learn_block_contents);
  size_t n = static_cast<size_t>(footer.learned_handle().size());
  char* buf = new char[n];
  Slice contents;
  s = file->Read(footer.learned_handle().offset(), n, &contents, buf);
  // std::cout << __func__ << " n_size: " << n << std::endl;
  // std::cout << __func__ << " footer.learned_handle().offset(): " << footer.learned_handle().offset() << std::endl;
  // std::cout << __func__ << " contents_size: " << contents.size() << std::endl;
  // std::cout << __func__ << " contents: " << string(contents.data(),contents.size()) << std::endl;
  RMIConfig rmi_config;
  RMIConfig::StageConfig first, second;

  first.model_type = RMIConfig::StageConfig::LinearRegression;
  first.model_n = 1;

  second.model_n = 1000;
  second.model_type = RMIConfig::StageConfig::LinearRegression;
  rmi_config.stage_configs.push_back(first);
  rmi_config.stage_configs.push_back(second);

  // LearnedRangeIndexSingleKey<uint64_t,float> learnmod(contents.data(), rmi_config);

  // Read the index block
  BlockContents index_block_contents;
  if (s.ok()) {
    ReadOptions opt;
    if (options.paranoid_checks) {
      opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);
  }

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->block_pos.clear();
    Iterator* iiter = index_block->NewIterator(options.comparator);
    int couter = 0;
    for (iiter->SeekToFirst(); iiter->Valid(); iiter->Next()) {
      Slice handle_value = iiter->value();
      BlockHandle handle;
      handle.DecodeFrom(&handle_value);
      // std::cout << __func__ << " num: " << couter++ << " ;push_back: " << handle.offset() << " ;push_back: " << handle.size() << std::endl;
      rep->block_pos.push_back({handle.offset(),handle.size()});
    }
    delete iiter;

    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    rep->learnedMod = new LearnedRangeIndexSingleKey<uint64_t,float> (string(contents.data(),contents.size()), rmi_config);
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  }

  return s;
}

void Table::ReadMeta(const Footer& footer) {
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::ModelBlockReader(void* arg, const ReadOptions& options,
                             uint64_t offset, uint64_t size) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  // BlockHandle handle;
  // Slice input = index_value;
  // Status s = handle.DecodeFrom(&input);
  Status s;
  if (true) {
    BlockContents contents;
    if (block_cache != nullptr) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, offset);
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        s = ModelReadBlock(table->rep_->file, options, offset, size, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ModelReadBlock(table->rep_->file, options, offset, size, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::ModelGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)){
  // std::cout << " ModelGet "<< std::endl;
  Status s;
  Slice nkey (k.data(),8);
  uint64_t lekey = 0;
  sscanf(nkey.data(), "%8lld", &lekey);
  auto value_get = rep_->learnedMod->get(lekey);
  int block_num = value_get / 4096;

  FilterBlockReader* filter = rep_->filter;
    // BlockHandle handle;
  if (filter != nullptr  &&
        !filter->KeyMayMatch(rep_->block_pos[block_num].first, k)) {
      // Not found
  } else {
    // std::cout << __func__ << " ModelGet_offset: " << rep_->block_pos[block_num].first << " ;ModelGet_size: " << rep_->block_pos[block_num].second << std::endl;
    Iterator* block_iter = ModelBlockReader(this, options, rep_->block_pos[block_num].first, rep_->block_pos[block_num].second);
    block_iter->Seek(k);
    if (block_iter->Valid()) {
      (*handle_result)(arg, block_iter->key(), block_iter->value());
    }
    s = block_iter->status();
    delete block_iter;
  }
  // if (s.ok()) {
  //   s = iiter->status();
  // }
  return s;
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
    } else {
      handle.DecodeFrom(&handle_value);
      Slice nkey (k.data(),8);
      uint64_t lekey = 0;
      sscanf(nkey.data(), "%8lld", &lekey);
      auto value_get = rep_->learnedMod->get(lekey);
      int block_num = value_get / 4096;
      if (rep_->block_pos[block_num].first != handle.offset())
        std::cout << __func__ << " no find key: " << lekey << " ;block_num:" << block_num << " ;value_get: "<< value_get << std::endl;
            // <<  " ;ModelGet_offset: " << rep_->block_pos[block_num].first  
            // <<  " ;handle_offset: " << handle.offset() << std::endl;
      

      // std::cout << __func__ << " handle_offset: " << handle.offset() << " ;handle_size: " << handle.size() << std::endl;

      // std::cout << __func__ << " lekey: " << lekey << " ;value_get: " << value_get << " ;block_num: " << block_num << std::endl;
      // std::cout << __func__ << " ModelGet_offset: " << rep_->block_pos[block_num].first << " ;ModelGet_size: " << rep_->block_pos[block_num].second << std::endl;
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
