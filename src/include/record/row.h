#ifndef MINISQL_ROW_H
#define MINISQL_ROW_H

#include <memory>
#include <vector>
#include "common/macros.h"
#include "common/rowid.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/mem_heap.h"

/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *  
 *
 */
class Row {
public:
  /**
   * Row used for insert
   * Field integrity should check by upper level
   */
  explicit Row(std::vector<Field> &fields) : heap_(new SimpleMemHeap) {
    // deep copy -> fields keep the same order as the argument vector.
    for (auto &field : fields) {
      void *buf = heap_->Allocate(sizeof(Field));
      fields_.push_back(new(buf)Field(field));
    }
  }

  /**
   * Row used for deserialize
   */
  Row() = delete; // default ctor can not be called, this ctor is deleted.

  /** 
   * Row used for deserialize and update
   */
  Row(RowId rid) : rid_(rid), heap_(new SimpleMemHeap) {}

  /**
   * Row copy function -> deep copy, so need to new a heap storage.
   */
  Row(const Row &other) : heap_(new SimpleMemHeap) {
    if (!fields_.empty()) { 
      // if there are still fields left in the definition row. 
      // First clear the old fields out before copying the new fields in.
      // note that the delete is deep delete like the deep copy, need to deallocate the fields' space first
      // then clear the vector out (vector just save the fields' pointers)
      for (auto &field : fields_) {
        heap_->Free(field);
      }
      fields_.clear();
    }
    rid_ = other.rid_;
    // do the deep copy
    for (auto &field : other.fields_) {
      //Allocate space for the Field Size
      void *buf = heap_->Allocate(sizeof(Field));
      //Create the class field on the locations of the buf on the memory.
      //Write locations into the fileds vector.
      fields_.push_back(new(buf)Field(*field));
    }
  }

  virtual ~Row() {
    delete heap_; // delete the memory management.
  }

  /**
   * Note: Make sure that bytes write to buf is equal to GetSerializedSize()
   */
  uint32_t SerializeTo(char *buf, Schema *schema) const;

  uint32_t DeserializeFrom(char *buf, Schema *schema);

  /**
   * For empty row, return 0
   * For non-empty row with null fields, eg: |null|null|null|, return header size only
   * @return
   */
  uint32_t GetSerializedSize(Schema *schema) const;

  inline const RowId GetRowId() const { return rid_; }

  inline void SetRowId(RowId rid) { rid_ = rid; }
   
  inline  std::vector<Field *>& GetFields() { return fields_; }

  inline Field *GetField(uint32_t idx) const {
    ASSERT(idx < fields_.size(), "Failed to access field");
    return fields_[idx];
  }

  inline size_t GetFieldCount() const { return fields_.size(); }

private:
  Row &operator=(const Row &other) = delete; // the default operator= is banned

private:
  RowId rid_{}; // stores the RowId of the row,format-> | (high 32 bits) page_id | (low 32 bits) slot_num |
  std::vector<Field *> fields_;   /** Make sure that all fields are created by mem heap */
  MemHeap *heap_{nullptr}; // the storage of row.
};

#endif //MINISQL_TUPLE_H
