//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/compaction/compaction_picker.h"

namespace rocksdb {
// Picking compactions for QuickSand Compaction
  class QuickSandCompactionPicker : public CompactionPicker {
  private:
    autovector<FdWithKeyRange> *quicksandfiles;
  public:
    QuickSandCompactionPicker(const ImmutableCFOptions &ioptions,
                              const InternalKeyComparator *icmp)
      : CompactionPicker(ioptions, icmp) {
      quicksandfiles = new autovector<FdWithKeyRange>();
      quicksandfiles->clear();
    }

    ~QuickSandCompactionPicker() {
      if (quicksandfiles != nullptr) {
        delete quicksandfiles;
      }
    }

    virtual Compaction *PickCompaction(const std::string &cf_name,
                                       const MutableCFOptions &mutable_cf_options,
                                       VersionStorageInfo *vstorage,
                                       LogBuffer *log_buffer) override;

    virtual bool NeedsCompaction(
      const VersionStorageInfo *vstorage) const override;

    bool CollectQuickSand(const VersionStorageInfo *vstorage) const;

    autovector<FdWithKeyRange> *GetQuickSandList();
  };

}  // namespace rocksdb
