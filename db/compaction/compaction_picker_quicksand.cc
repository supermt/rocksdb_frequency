//
// Created by jinghuayu2 on 17/2/2020.
//
// DOTA framework specific compaction style

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <utility>
#include <vector>

#include "db/compaction/compaction_picker_quicksand.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace rocksdb {
  namespace {
    class QuicksandCompactionBuilder {
    public:
      QuicksandCompactionBuilder(const std::string &cf_name,
                                 VersionStorageInfo *vstorage,
                                 CompactionPicker *compaction_picker,
                                 LogBuffer *log_buffer,
                                 const MutableCFOptions &mutable_cf_options,
                                 const ImmutableCFOptions &ioptions)
        : cf_name_(cf_name),
          vstorage_(vstorage),
          compaction_picker_(compaction_picker),
          log_buffer_(log_buffer),
          mutable_cf_options_(mutable_cf_options),
          ioptions_(ioptions) {}

      const std::string &cf_name_;
      VersionStorageInfo *vstorage_;
      CompactionPicker *compaction_picker_;
      LogBuffer *log_buffer_;
      int start_level_ = -1;
      int output_level_ = -1;
      int parent_index_ = -1;
      int base_index_ = -1;
      double start_level_score_ = 0;
      autovector<FdWithKeyRange> quicksand_files_;
      bool is_manual_ = false;
      CompactionInputFiles start_level_inputs_;
      std::vector<CompactionInputFiles> compaction_inputs_;
      CompactionInputFiles output_level_inputs_;
      std::vector<FileMetaData *> grandparents_;
      CompactionReason compaction_reason_ = CompactionReason::kUnknown;

      const MutableCFOptions &mutable_cf_options_;
      const ImmutableCFOptions &ioptions_;

      // Methods
      Compaction *PickCompaction(autovector<FdWithKeyRange> * overlapped_files);

      void PassQuickSandFiles(autovector<FdWithKeyRange> *quicksandfiles) {
        this->quicksand_files_ = autovector<FdWithKeyRange>(*quicksandfiles);
      }

      uint32_t GetPathId(const ImmutableCFOptions &ioptions,
                         const MutableCFOptions &mutable_cf_options, int level);
    };


    uint32_t QuicksandCompactionBuilder::GetPathId(
      const ImmutableCFOptions& ioptions,
      const MutableCFOptions& mutable_cf_options, int level) {
      uint32_t p = 0;
      assert(!ioptions.cf_paths.empty());
      assert(mutable_cf_options.max_bytes_for_level_base > 0);
      if (mutable_cf_options.max_bytes_for_level_base <= 0) {
        return 0;
      }
      p = level / 2;
      uint32_t max_path = ioptions.cf_paths.size() - 1;
      return p >= max_path ? max_path : p;
    }

    // implementations
    Compaction *QuicksandCompactionBuilder::PickCompaction
    (autovector<FdWithKeyRange> * overlapped_files) {
      // jinghuan: Select the output level.
      PassQuickSandFiles(overlapped_files);

      auto newCompaction = new Compaction(
        vstorage_, ioptions_, mutable_cf_options_, std::move(compaction_inputs_),
        output_level_,
        MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                            ioptions_.compaction_style, vstorage_->base_level(),
                            ioptions_.level_compaction_dynamic_level_bytes),
        mutable_cf_options_.max_compaction_bytes,
        GetPathId(ioptions_, mutable_cf_options_, output_level_),
        GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                           output_level_, vstorage_->base_level()),
        GetCompressionOptions(ioptions_, vstorage_, output_level_),
        /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
        start_level_score_, false /* deletion_compaction */, compaction_reason_);

      compaction_picker_->RegisterCompaction(newCompaction);
      return nullptr;
    }

  } // end of builders

  Compaction *
  QuickSandCompactionPicker::PickCompaction(const std::string &cf_name,
                                            const rocksdb::MutableCFOptions &mutable_cf_options,
                                            rocksdb::VersionStorageInfo *vstorage,
                                            rocksdb::LogBuffer *log_buffer) {
    QuicksandCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                       mutable_cf_options, ioptions_);
    CollectQuickSand(vstorage);
    return builder.PickCompaction(this->GetQuickSandList());
  }

  bool QuickSandCompactionPicker::CollectQuickSand(const
                                                   rocksdb::VersionStorageInfo *vstorage) const {
    // jinghuan: re-calculate the QuickSand Score
    LevelFilesBrief deepest_level_files = vstorage->LevelFilesBrief(
      vstorage->num_non_empty_levels());

    for (size_t i = 0; i < deepest_level_files.num_files; i++) {
      this->quicksandfiles->push_back(deepest_level_files.files[i]);
    }

    return quicksandfiles->empty();
  }

  autovector<FdWithKeyRange> *QuickSandCompactionPicker::GetQuickSandList() {
    return this->quicksandfiles;
  }


  bool rocksdb::QuickSandCompactionPicker::NeedsCompaction(
    // before calculating the compaction score, we should prevent the quicksand
    // use the builder to do the calculation.
    const rocksdb::VersionStorageInfo *vstorage) const {
    CollectQuickSand(vstorage);
    if (!quicksandfiles->empty()) {
      return true;
    }
    if (!vstorage->ExpiredTtlFiles().empty()) {
      return true;
    }
    if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
      return true;
    }
    if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
      return true;
    }
    if (!vstorage->FilesMarkedForCompaction().empty()) {
      return true;
    }
    // jinghuan: is there any other compaction reason?
    for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
      if (vstorage->CompactionScore(i) >= 1) {
        return true;
      }
    }
    return false;
  }

}
