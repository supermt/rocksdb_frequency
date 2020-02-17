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
      bool is_manual_ = false;
      CompactionInputFiles start_level_inputs_;
      std::vector<CompactionInputFiles> compaction_inputs_;
      CompactionInputFiles output_level_inputs_;
      std::vector<FileMetaData *> grandparents_;
      CompactionReason compaction_reason_ = CompactionReason::kUnknown;

      const MutableCFOptions &mutable_cf_options_;
      const ImmutableCFOptions &ioptions_;

      // Methods

      Compaction *PickCompaction();
    };

    Compaction * QuicksandCompactionBuilder::PickCompaction() {
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
    return builder.PickCompaction();
  }

  bool rocksdb::QuickSandCompactionPicker::NeedsCompaction(
    const rocksdb::VersionStorageInfo *vstorage) const {
    return false;
  }

}
