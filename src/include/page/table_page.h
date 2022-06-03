#ifndef MINISQL_TUPLE_H
#define MINISQL_TUPLE_H
/**
 * Basic Slotted page format:
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | PageId (4) | LSN (4)(not used, but keep this field in the header) | PrevPageId (4) | NextPageId (4) |
 *FreeSpacePointer(4) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
 *  ----------------------------------------------------------------
 **/

#include <cstring>
#include "common/macros.h"
#include "common/rowid.h"
#include "page/page.h"
#include "record/row.h"

// transaction is not used here.
#include "transaction/lock_manager.h"
#include "transaction/log_manager.h"
#include "transaction/transaction.h"

// error type definition.
#define INVALID_SLOT_NUMBER -1
#define TUPLE_DELETED -2
#define NOT_ENOUGH_SPACE -3

class TablePage : public Page {
 public:
  void Init(page_id_t page_id, page_id_t prev_id, LogManager *log_mgr, Transaction *txn);

  page_id_t GetTablePageId() { return *reinterpret_cast<page_id_t *>(GetData()); }

  page_id_t GetPrevPageId() { return *reinterpret_cast<page_id_t *>(GetData() + OFFSET_PREV_PAGE_ID); }

  page_id_t GetNextPageId() { return *reinterpret_cast<page_id_t *>(GetData() + OFFSET_NEXT_PAGE_ID); }

  void SetPrevPageId(page_id_t prev_page_id) {
    memcpy(GetData() + OFFSET_PREV_PAGE_ID, &prev_page_id, sizeof(page_id_t));
  }

  void SetNextPageId(page_id_t next_page_id) {
    memcpy(GetData() + OFFSET_NEXT_PAGE_ID, &next_page_id, sizeof(page_id_t));
  }

  bool InsertTuple(Row &row, Schema *schema, Transaction *txn, LockManager *lock_manager, LogManager *log_manager);

  bool MarkDelete(const RowId &rid, Transaction *txn, LockManager *lock_manager, LogManager *log_manager);

  int UpdateTuple(const Row &new_row, Row *old_row, Schema *schema, Transaction *txn, LockManager *lock_manager,
                  LogManager *log_manager);

  void ApplyDelete(const RowId &rid, Transaction *txn, LogManager *log_manager);

  void RollbackDelete(const RowId &rid, Transaction *txn, LogManager *log_manager);

  bool GetTuple(Row *row, Schema *schema, Transaction *txn, LockManager *lock_manager);

  bool GetFirstTupleRid(RowId *first_rid);

  bool GetNextTupleRid(const RowId &cur_rid, RowId *next_rid);

  uint32_t position_calculate(uint32_t slot_num) {
    // this function calculate actual tuple offset (slot_num is the tuple index in the slotted-page.)
    return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_TUPLE_OFFSET + SIZE_TUPLE * slot_num);
  }

 public:
  uint32_t GetFreeSpacePointer() { return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_FREE_SPACE); }

  void SetFreeSpacePointer(uint32_t free_space_pointer) {  // stores the value of free_space_pointer(not actually a
                                                           // pointer, but an offset from the head of the page instead.)
    memcpy(GetData() + OFFSET_FREE_SPACE, &free_space_pointer, sizeof(uint32_t));
  }

  uint32_t GetTupleCount() { return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_TUPLE_COUNT); }

  void SetTupleCount(uint32_t tuple_count) { memcpy(GetData() + OFFSET_TUPLE_COUNT, &tuple_count, sizeof(uint32_t)); }

  uint32_t GetFreeSpaceRemaining() {
    // this function can get the current free space counted by bytes.
    return GetFreeSpacePointer() - SIZE_TABLE_PAGE_HEADER - SIZE_TUPLE * GetTupleCount();
  }

  uint32_t GetTupleOffsetAtSlot(uint32_t slot_num) {
    // this function calculate actual tuple offset (slot_num is the tuple index in the slotted-page.)
    return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_TUPLE_OFFSET + SIZE_TUPLE * slot_num);
  }

  void SetTupleOffsetAtSlot(uint32_t slot_num, uint32_t offset) {
    // change the slot_num correspondence tuple offset to offset.
    memcpy(GetData() + OFFSET_TUPLE_OFFSET + SIZE_TUPLE * slot_num, &offset, sizeof(uint32_t));
  }

  uint32_t GetTupleSize(uint32_t slot_num) {
    // get the corresponding tuple's size (corresponding -> slot_num is the tuple index in the slotted-page.)
    return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_TUPLE_SIZE + SIZE_TUPLE * slot_num);
  }

  void SetTupleSize(uint32_t slot_num, uint32_t size) {
    // the same place as the upper function.
    memcpy(GetData() + OFFSET_TUPLE_SIZE + SIZE_TUPLE * slot_num, &size, sizeof(uint32_t));
  }

  // the input argument tuple_size -> when the size is zero or the bit(index-31) is 1 <delete flag>, then it is deleted.
  static bool IsDeleted(uint32_t tuple_size) { return static_cast<bool>(tuple_size & DELETE_MASK) || tuple_size == 0; }

  // Set the delete flag to be 1 (bit index of delete flag is index-31 bit)
  static uint32_t SetDeletedFlag(uint32_t tuple_size) { return static_cast<uint32_t>(tuple_size | DELETE_MASK); }

  // unset deleted flag -> Set the delete flag to be 0 (bit index of 31 is set to zero.)
  static uint32_t UnsetDeletedFlag(uint32_t tuple_size) { return static_cast<uint32_t>(tuple_size & (~DELETE_MASK)); }

 private:
  // all these const expressions are calculated by using the bytes as the unit.
  static_assert(sizeof(page_id_t) == 4);  // indicate that the page_id_t has a 4-byte length.
  static constexpr uint64_t DELETE_MASK = (1U << (8 * sizeof(uint32_t) - 1));  // | 32 bits | 1 appends 31-bits 0 |
  static constexpr size_t SIZE_TABLE_PAGE_HEADER =
      24;  // 4 * 6, The basic header size without calculation of tuples' offset and tuples' size
  static constexpr size_t SIZE_TUPLE = 8;            // a tuple size needed in the header. -> offset(4) | size(4)
  static constexpr size_t OFFSET_PREV_PAGE_ID = 8;   // the start place of PREV_PAGE_ID in the header.
  static constexpr size_t OFFSET_NEXT_PAGE_ID = 12;  // the start place of NEXT_PAGE_ID in the header.
  static constexpr size_t OFFSET_FREE_SPACE = 16;    // the start place of Free_Space_Pointer in the slotted-page.
  static constexpr size_t OFFSET_TUPLE_COUNT = 20;   // the start place of TupleCount in the header.
  static constexpr size_t OFFSET_TUPLE_OFFSET =
      24;  // the actual records of tuples in the header. (where the first tuple's offset starts)
  static constexpr size_t OFFSET_TUPLE_SIZE = 28;  // where the first tuple's size starts.

 public:
  static constexpr size_t SIZE_MAX_ROW = PAGE_SIZE - SIZE_TABLE_PAGE_HEADER -
                                         SIZE_TUPLE;  // 4kBytes - basic header size - first tuple space in the header.
};

#endif