#include "record/schema.h"
#include <iostream>
using namespace std;

uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t ofs = 0;
  std::vector<Column *> columns_ = this->GetColumns();
  
  //1.Write the Magic Number
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  ofs += sizeof(uint32_t);

  //2.Write the size of the columns
  MACH_WRITE_UINT32(buf + ofs, (columns_.size()));
  ofs += sizeof(uint32_t);

  //3.Write the Columns the into the buf
  for (uint32_t i = 0; i < columns_.size(); i++) {
    //Write the Serialized Size of the Each column
    MACH_WRITE_UINT32(buf + ofs, (columns_[i]->GetSerializedSize()));
    ofs += sizeof(uint32_t);
    //Write the Serialized Column into the buf
    columns_[i]->SerializeTo(buf + ofs);
    ofs += columns_[i]->GetSerializedSize();
  }
  
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  std::vector<Column *> columns_ = this->GetColumns();
  uint32_t LengthOfTable = columns_.size();
  uint32_t Size = 0;
  //1.Calculate the Magic Number and SizeOf(Columns)
  Size += 2 * sizeof(uint32_t);
  //2.Calculate the Total Column
  for (uint32_t i = 0; i < LengthOfTable; i++) {
    //The SerializedSize
    Size += sizeof(uint32_t);
    Size += columns_[i]->GetSerializedSize();
  }

  return Size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // First if buf is nullptr, then nothing to deserialize from. And the returned offset is 0 as well.
  if (buf == nullptr) return 0;

  // Do the actual deserialization work.
  uint32_t ofs = 0;
  std::vector<Column *> columns_; // Which will be used to construct the schema

  // 1. Read the Magic_Number
  uint32_t Magic_Number = MACH_READ_FROM(uint32_t, (buf));
  ofs += sizeof(uint32_t);
  
  // If does not match---Error
  ASSERT(Magic_Number == 200715, "MagicNumber does not match in schema deserialization");
  /** do the check of Magic_number.
  if (Magic_Number != 200715) {
    std::cerr << "MagicNumber does not match" << std::endl;
  }
  */
  // 2. Read the SizeOfColumns From the buf
  uint32_t LengthOfTable = MACH_READ_FROM(uint32_t, (buf + ofs));
  ofs += sizeof(uint32_t);
  // 3. Read the Columns in the Schema
  for (uint32_t i = 0; i < LengthOfTable; i++) {
    ofs += sizeof(uint32_t); // read the size of attributes out (actually redundant.)
    Column *tmp = nullptr;
    ofs += Column::DeserializeFrom(buf + ofs, tmp, heap);
    columns_.push_back(tmp);
  }
  void *mem = heap->Allocate(sizeof(Schema));
  schema = new (mem)Schema(columns_);
  return ofs;
}