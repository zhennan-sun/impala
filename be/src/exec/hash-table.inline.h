// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXEC_HASH_TABLE_INLINE_H
#define IMPALA_EXEC_HASH_TABLE_INLINE_H

#include "exec/hash-table.h"

namespace impala {

<<<<<<< HEAD
inline HashTable::Iterator HashTable::Find(TupleRow* probe_row) {
  bool has_nulls = EvalProbeRow(probe_row);
  if (!stores_nulls_ && has_nulls) return End();
  uint32_t hash = HashCurrentRow();
  int64_t bucket_idx = hash % num_buckets_;

  Bucket* bucket = &buckets_[bucket_idx];
  int64_t node_idx = bucket->node_idx_;
  while (node_idx != -1) {
    Node* node = GetNode(node_idx);
    if (node->hash_ == hash && Equals(node->data())) {
      return Iterator(this, bucket_idx, node_idx, hash);
    }
    node_idx = node->next_idx_;
  }

  return End();
}
  
inline HashTable::Iterator HashTable::Begin() {
  int64_t bucket_idx = -1;
  Bucket* bucket = NextBucket(&bucket_idx);
  if (bucket != NULL) {
    return Iterator(this, bucket_idx, bucket->node_idx_, 0);
=======
inline bool HashTableCtx::EvalAndHashBuild(TupleRow* row, uint32_t* hash) {
  bool has_null = EvalBuildRow(row);
  if (!stores_nulls_ && has_null) return false;
  *hash = HashCurrentRow();
  return true;
}

inline bool HashTableCtx::EvalAndHashProbe(TupleRow* row, uint32_t* hash) {
  bool has_null = EvalProbeRow(row);
  if ((!stores_nulls_ || !finds_nulls_) && has_null) return false;
  *hash = HashCurrentRow();
  return true;
}

inline HashTable::Iterator HashTable::Find(HashTableCtx* ht_ctx, uint32_t hash) {
  DCHECK_NOTNULL(ht_ctx);
  DCHECK_NE(num_buckets_, 0);
  DCHECK_EQ(hash, ht_ctx->HashCurrentRow());
  int64_t bucket_idx = hash & (num_buckets_ - 1);
  Bucket* bucket = &buckets_[bucket_idx];
  Node* node = bucket->node;
  while (node != NULL) {
    if (node->hash == hash && ht_ctx->Equals(GetRow(node, ht_ctx->row_))) {
      return Iterator(this, ht_ctx, bucket_idx, node, hash);
    }
    node = node->next;
  }
  return End();
}

inline HashTable::Iterator HashTable::Begin(HashTableCtx* ctx) {
  DCHECK_NE(num_buckets_, 0);
  int64_t bucket_idx = -1;
  Bucket* bucket = NextBucket(&bucket_idx);
  if (bucket != NULL) return Iterator(this, ctx, bucket_idx, bucket->node, 0);
  return End();
}

inline HashTable::Iterator HashTable::FirstUnmatched(HashTableCtx* ctx) {
  int64_t bucket_idx = -1;
  Bucket* bucket = NextBucket(&bucket_idx);
  while (bucket != NULL) {
    Node* node = bucket->node;
    while (node != NULL && node->matched) {
      node = node->next;
    }
    if (node == NULL) {
      bucket = NextBucket(&bucket_idx);
    } else {
      DCHECK(!node->matched);
      return Iterator(this, ctx, bucket_idx, node, 0);
    }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return End();
}

inline HashTable::Bucket* HashTable::NextBucket(int64_t* bucket_idx) {
  ++*bucket_idx;
  for (; *bucket_idx < num_buckets_; ++*bucket_idx) {
<<<<<<< HEAD
    if (buckets_[*bucket_idx].node_idx_ != -1) return &buckets_[*bucket_idx];
=======
    if (buckets_[*bucket_idx].node != NULL) return &buckets_[*bucket_idx];
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  *bucket_idx = -1;
  return NULL;
}

<<<<<<< HEAD
inline void HashTable::InsertImpl(TupleRow* row) {
  bool has_null = EvalBuildRow(row);
  if (!stores_nulls_ && has_null) return;

  uint32_t hash = HashCurrentRow();
  int64_t bucket_idx = hash % num_buckets_;
  if (num_nodes_ == nodes_capacity_) GrowNodeArray();
  Node* node = GetNode(num_nodes_);
  TupleRow* data = node->data();
  node->hash_ = hash;
  memcpy(data, row, sizeof(Tuple*) * num_build_tuples_);
  AddToBucket(&buckets_[bucket_idx], num_nodes_, node);
  ++num_nodes_;
}

inline void HashTable::AddToBucket(Bucket* bucket, int64_t node_idx, Node* node) {
  if (bucket->node_idx_ == -1) ++num_filled_buckets_;
  node->next_idx_ = bucket->node_idx_;
  bucket->node_idx_ = node_idx;
}

template<bool check_match>
inline void HashTable::Iterator::Next() {
  if (bucket_idx_ == -1) return;

  // TODO: this should prefetch the next tuplerow
  Node* node = table_->GetNode(node_idx_);
=======
inline bool HashTable::Insert(HashTableCtx* ht_ctx,
    const BufferedTupleStream::RowIdx& idx, TupleRow* row, uint32_t hash) {
  Node* node = InsertImpl(ht_ctx, hash);
  if (node == NULL) return false;
  if (stores_tuples_) {
    DCHECK_EQ(num_build_tuples_, 1);
    // Optimization: if this row is just a single tuple, just store the tuple*.
    node->tuple = row->GetTuple(0);
  } else {
    node->idx = idx;
  }
  return true;
}

inline bool HashTable::Insert(HashTableCtx* ht_ctx, Tuple* tuple, uint32_t hash) {
  DCHECK(stores_tuples_);
  Node* node = InsertImpl(ht_ctx, hash);
  if (node == NULL) return false;
  node->tuple = tuple;
  return true;
}

inline HashTable::Node* HashTable::InsertImpl(HashTableCtx* ht_ctx, uint32_t hash) {
  DCHECK_NOTNULL(ht_ctx);
  DCHECK_NE(num_buckets_, 0);
  if (UNLIKELY(num_filled_buckets_ > num_buckets_till_resize_)) {
    // TODO: next prime instead of double?
    if (!ResizeBuckets(num_buckets_ * 2)) return NULL;
  }
  if (node_remaining_current_page_ == 0) {
    if (!GrowNodeArray()) return NULL;
  }
  DCHECK_EQ(hash, ht_ctx->HashCurrentRow());
  int64_t bucket_idx = hash & (num_buckets_ - 1);
  next_node_->hash = hash;
  next_node_->matched = false;
  AddToBucket(&buckets_[bucket_idx], next_node_);
  DCHECK_GT(node_remaining_current_page_, 0);
  --node_remaining_current_page_;
  ++num_nodes_;
  return next_node_++;
}

inline void HashTable::AddToBucket(Bucket* bucket, Node* node) {
  num_filled_buckets_ += (bucket->node == NULL);
  node->next = bucket->node;
  bucket->node = node;
}

inline void HashTable::MoveNode(Bucket* from_bucket, Bucket* to_bucket,
    Node* node, Node* previous_node) {
  if (previous_node != NULL) {
    previous_node->next = node->next;
  } else {
    // Update bucket directly
    from_bucket->node = node->next;
    num_filled_buckets_ -= (node->next == NULL);
  }
  AddToBucket(to_bucket, node);
}

template<bool check_match>
inline void HashTable::Iterator::Next(HashTableCtx* ht_ctx) {
  if (bucket_idx_ == -1) return;
  DCHECK_NOTNULL(ht_ctx);

  // TODO: this should prefetch the next tuplerow
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Iterator is not from a full table scan, evaluate equality now.  Only the current
  // bucket needs to be scanned. 'expr_values_buffer_' contains the results
  // for the current probe row.
  if (check_match) {
    // TODO: this should prefetch the next node
<<<<<<< HEAD
    int64_t next_idx = node->next_idx_;
    while (next_idx != -1) {
      node = table_->GetNode(next_idx);
      if (node->hash_ == scan_hash_ && table_->Equals(node->data())) {
        node_idx_ = next_idx;
        return;
      } 
      next_idx = node->next_idx_;
=======
    Node* node = node_->next;
    while (node != NULL) {
      if (node->hash == scan_hash_ &&
          ht_ctx->Equals(table_->GetRow(node, ht_ctx->row_))) {
        node_ = node;
        return;
      }
      node = node->next;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
    *this = table_->End();
  } else {
    // Move onto the next chained node
<<<<<<< HEAD
    if (node->next_idx_ != -1) {
      node_idx_ = node->next_idx_;
=======
    if (node_->next != NULL) {
      node_ = node_->next;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      return;
    }

    // Move onto the next bucket
    Bucket* bucket = table_->NextBucket(&bucket_idx_);
    if (bucket == NULL) {
      bucket_idx_ = -1;
<<<<<<< HEAD
      node_idx_ = -1;
    } else {
      node_idx_ = bucket->node_idx_;
=======
      node_ = NULL;
    } else {
      node_ = bucket->node;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }
}

<<<<<<< HEAD
=======
inline bool HashTable::Iterator::NextUnmatched() {
  if (bucket_idx_ == -1) return false;
  while (true) {
    while (node_->next != NULL && node_->next->matched) {
      node_ = node_->next;
    }
    if (node_->next == NULL) {
      // Move onto the next bucket.
      Bucket* bucket = table_->NextBucket(&bucket_idx_);
      if (bucket == NULL) {
        bucket_idx_ = -1;
        node_ = NULL;
        return false;
      } else {
        node_ = bucket->node;
        if (node_ != NULL && !node_->matched) return true;
      }
    } else {
      DCHECK(!node_->next->matched);
      node_ = node_->next;
      return true;
    }
  }
}

inline void HashTableCtx::set_level(int level) {
  DCHECK_GE(level, 0);
  DCHECK_LT(level, seeds_.size());
  level_ = level;
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

#endif
