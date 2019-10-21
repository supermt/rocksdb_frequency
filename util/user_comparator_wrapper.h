// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"

#define MIN(x, y) ((x) < (y) ? (x) : (y))
#define MAX(x, y) ((x) > (y) ? (x) : (y))

namespace rocksdb {
class LevenshteinDistanceCalculator {
 public:
  static int Calculate(const Slice& a, const Slice& b) {
    int i, j, l1, l2, t, track;
    int dist_buffer_len = MAX(a.size(), b.size());
    int dist[dist_buffer_len][dist_buffer_len];
    l1 = a.size_;
    l2 = b.size_;

    for (i = 0; i <= l1; i++) {
      dist[0][i] = i;
    }
    for (j = 0; j <= l2; j++) {
      dist[j][0] = j;
    }
    for (j = 1; j <= l1; j++) {
      for (i = 1; i <= l2; i++) {
        if (a.data_[i - 1] == a.data_[j - 1]) {
          track = 0;
        } else {
          track = 1;
        }
        t = MIN((dist[i - 1][j] + 1), (dist[i][j - 1] + 1));
        dist[i][j] = MIN(t, (dist[i - 1][j - 1] + track));
      }
    }
    return (a.compare(b)) * (dist[l2][l1]);
  }
};
// Wrapper of user comparator, with auto increment to
// perf_context.user_key_comparison_count.
class UserComparatorWrapper final : public Comparator {
 public:
  explicit UserComparatorWrapper(const Comparator* const user_cmp)
      : user_comparator_(user_cmp) {}

  ~UserComparatorWrapper() = default;

  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const Slice& a, const Slice& b) const override {
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    return user_comparator_->Compare(a, b);
  }

  bool Equal(const Slice& a, const Slice& b) const override {
    PERF_COUNTER_ADD(user_key_comparison_count, 1);
    return user_comparator_->Equal(a, b);
  }

  const char* Name() const override { return user_comparator_->Name(); }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    return user_comparator_->FindShortestSeparator(start, limit);
  }

  void FindShortSuccessor(std::string* key) const override {
    return user_comparator_->FindShortSuccessor(key);
  }

  const Comparator* GetRootComparator() const override {
    return user_comparator_->GetRootComparator();
  }

  bool IsSameLengthImmediateSuccessor(const Slice& s,
                                      const Slice& t) const override {
    return user_comparator_->IsSameLengthImmediateSuccessor(s, t);
  }

  bool CanKeysWithDifferentByteContentsBeEqual() const override {
    return user_comparator_->CanKeysWithDifferentByteContentsBeEqual();
  }

  int Distance(const Slice& a, const Slice& b) {
    return LevenshteinDistanceCalculator::Calculate(a, b);
  }

 private:
  const Comparator* user_comparator_;
};
}  // namespace rocksdb
