#ifndef MINISQL_COLUMN_H
#define MINISQL_COLUMN_H

#include <string>

using namespace std;

#include "common/macros.h"
#include "record/types.h"

class Column {
  friend class Schema;

public:
  Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique); // ctor for non-char type

  Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique); // ctor for char type (with length)

  Column(const Column *other); // copy ctor

  std::string GetName() const { return name_; }

  uint32_t GetLength() const { return len_; }

  void SetTableInd(uint32_t ind) { table_ind_ = ind; }
  void SetNullable(bool state) { nullable_ = state; };
  void SetUnique(bool state) { unique_ = state; };

  uint32_t GetTableInd() const { return table_ind_; }

  bool IsNullable() const { return nullable_; }

  bool IsUnique() const { return unique_; }

  TypeId GetType() const { return type_; }

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, Column *&column, MemHeap *heap);

private:
  
  static constexpr uint32_t COLUMN_MAGIC_NUM = 210928; // ID number of column
  std::string name_;      // the name of column attribute
  TypeId type_;           // stores the type id of this attribute: int, float, char(and char array as well) supported now
  uint32_t len_{0};       // for char type this is the maximum byte length of the string data,
  // otherwise is the fixed size
  uint32_t table_ind_{0}; // column position in table (start from 0, 1, 2, ... record the index of attributes.)
  bool nullable_{false};  // whether the column can be null
  bool unique_{false};    // whether the column is unique
};

#endif //MINISQL_COLUMN_H
