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
struct Mod{
  int max_lenth;
  int str_size;
  std::vector<char> based_char;
  std::vector<double>based_num;
  std::vector<Segment> string_segments;

};
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
  Mod* learnedMod;
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  Block* index_block;
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

  BlockContents learn_block_contents;
  Mod* learnmod = new Mod;
  if (s.ok()) {
    // std::cout << __func__ << " n=" << std::endl;

    // ReadOptions opt;
    // s = ReadBlock(file, opt, footer.learned_handle(), &learn_block_contents);
    size_t n = static_cast<size_t>(footer.learned_handle().size());
    std::cout << __func__ << " " << n << std::endl;
    char* buf = new char[n];
    Slice contents;
    // std::cout << __func__ << " footer.learned_handle().offset()" << footer.learned_handle().offset() << std::endl;
    // std::cout << __func__ << " footer.index_handle().offset()" << footer.index_handle().offset() << std::endl;
    // std::cout << __func__ << " footer.index_handle().size()" << footer.index_handle().size() << std::endl;
    s = file->Read(footer.learned_handle().offset(), n, &contents, buf);
    // std::cout << __func__ << " file->Read over" << std::endl;

    // PrintBuffer(contents.data(), contents.size());
    // PrintBuffer(buf, n);
    // std::cout << __func__ << " " << (void*)buf << " " << (void*)contents.data() << std::endl;

    const char* src = contents.data();
    memcpy(&(learnmod->max_lenth), contents.data(), sizeof(learnmod->max_lenth));
    // std::cout << __func__ << " learnmod->max_lenth: " << learnmod->max_lenth << std::endl;

    src += sizeof(learnmod->max_lenth);
    for (int i = 0; i < learnmod->max_lenth; i++){
      char tmp;
      memcpy(&(tmp), src, sizeof(tmp));
      learnmod->based_char.push_back(tmp);
      src += sizeof(tmp);
      // std::cout << __func__ << " learnmod->based_char: " << learnmod->based_char[i] << std::endl;
    }
    for (int i = 0; i < learnmod->max_lenth; i++){
      double tmp = 0;
      memcpy(&(tmp), src, sizeof(tmp));
      learnmod->based_num.push_back(tmp);
      src += sizeof(tmp);
      // std::cout << __func__ << " learnmod->based_num: " << learnmod->based_num[i] << std::endl;
    }
    memcpy(&(learnmod->str_size), src, sizeof(learnmod->str_size));
    src += sizeof(learnmod->str_size);
    for (int i = 0; i < learnmod->str_size; i++){
      double x1 = 0;
      memcpy(&(x1), src, sizeof(x1));
      src += sizeof(x1);
      double x2 = 0;
      memcpy(&(x2), src, sizeof(x2));
      src += sizeof(x2);
      double x3 = 0;
      memcpy(&(x3), src, sizeof(x3));
      src += sizeof(x3);
      double x4 = 0;
      memcpy(&(x4), src, sizeof(x4));
      src += sizeof(x4);
      learnmod->string_segments.push_back((Segment) {x1, x2, x3, x4});
    }
  }


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
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    rep->learnedMod = learnmod;
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

Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

Status Table::ModelGet(const Slice& k){
  Status s;
  int size = rep_->learnedMod->max_lenth;

  double target_int = 0;
  for (int i = 0; i < size; i++){
    target_int += rep_->learnedMod->based_num[i] * (double)(k.data()[i] - rep_->learnedMod->based_char[i]);
  }

  // if (target_int > max_key) return std::make_pair(size, size);
  // if (target_int < min_key) return std::make_pair(size, size);

  uint32_t left = 0, right = (uint32_t) string_segments.size() - 1;
  while (left != right - 1) {
      uint32_t mid = (right + left) / 2;
      if (target_int < string_segments[mid].x) right = mid;
      else left = mid;
  }

  if (target_int > string_segments[left].x2) {
      assert(left != string_segments.size() - 2);
      ++left;
      target_int = string_segments[left].x;
  }
  double error = 10;
  double result = target_int * string_segments[left].k + string_segments[left].b;
  uint64_t lower = result - error > 0 ? (uint64_t) std::floor(result - error) : 0;
  uint64_t upper = (uint64_t) std::ceil(result + error);
  assert(lower < size); // return std::make_pair(size, size);
  upper = upper < size ? upper : size - 1;
  
  std::cout << __func__ << " lower: " << lower << ";upper: " <<  upper << std::endl;
  // size_t index_lower = lower / adgMod::block_num_entries;
  // size_t index_upper = upper / adgMod::block_num_entries;

  // int comp = tf->table->rep_->options.comparator->Compare(mid_key, k);
  // i = comp < 0 ? index_upper : index_lower;
  // size_t pos_block_lower = i == index_lower ? lower % adgMod::block_num_entries : 0;
  // size_t pos_block_upper = i == index_upper ? upper % adgMod::block_num_entries : adgMod::block_num_entries - 1;
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
