#include "record/row.h"
#include <iostream>

using namespace std;

uint32_t Row::SerializeTo(char *buf, Schema *schema) const { // schema not used actually.
  // Part1->Header
  uint32_t ofs = 0;
  //   1.Write the FieldNumber
  MACH_WRITE_UINT32(buf, this->GetFieldCount());
  ofs += sizeof(uint32_t);
  //   2.Write the NullBitMap
  string NullBitMap; // the null bitmap used here is not actually bitmap, but a string version simulation.
  for (uint32_t i = 0; i < this->GetFieldCount(); i++) {
    if (fields_[i]->IsNull()) {
      // this fields is null
      NullBitMap.push_back('\1');
    } else {
      NullBitMap.push_back('\0');
    }
  }
  MACH_WRITE_STRING(buf + ofs, NullBitMap);
  ofs += NullBitMap.length();
  // Part2->Field Part
  
  //   3.Write the Fields
  for (uint32_t i = 0; i < this->GetFieldCount(); i++) {
    if (!fields_[i]->IsNull()) {
      ofs += fields_[i]->SerializeTo(buf + ofs);
    }
  }

  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) { 
// schema is used to test for the compatibility. 
// And also for integrity of data (complete null data's information)
  uint32_t ofs = 0;
  // if buf is nullptr, nothing to deserialize from.
  if (buf == nullptr) return 0;
  // else do the actual deserialize.
  //   1.Read the Field Num First
  uint32_t FieldNum = MACH_READ_FROM(uint32_t, (buf));
  ASSERT(FieldNum == schema->GetColumnCount(), "Field Count does not match");
  /** can be used to replace the upper statement.
  if (FieldNum != schema->GetColumnCount()) {
    std::cerr << "Field Count does not match" << endl;
  }
  */
  ofs += sizeof(uint32_t);
  //   2.Read the NullBitMap
  string NullBitMap;
  for (uint32_t i = 0; i < FieldNum; i++) {
    NullBitMap.push_back(*(buf+ofs+i));
  }
  ofs += FieldNum;
  
  //   3.Get the Field
  std::vector<Column *> Column = schema->GetColumns(); 
  for (uint32_t i = 0; i < FieldNum; i++) {
    if (NullBitMap[i] == '\1') {
      //it means that this field is null
      Field *tmp = nullptr;
      void *mem = heap_->Allocate(sizeof(Field));
      tmp = new (mem) Field(Column[i]->GetType());
      this->fields_.push_back(tmp);
    } else {
      Field *tmp = nullptr;
      ofs += Field::DeserializeFrom(buf + ofs, Column[i]->GetType(), &tmp, false, heap_);
      this->fields_.push_back(tmp);
    }
  }

  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  //1.Calculate the FieldNumber
  uint32_t ofs = 0;
  ofs += sizeof(uint32_t);

  //2.Calculate the Sizeof NullBitMap
  ofs += this->GetFieldCount();
  
  //3.Calculate the Size of the Field
  for (uint32_t i = 0; i < this->GetFieldCount(); i++) {
    if (!fields_[i]->IsNull()) {
      ofs += fields_[i]->GetSerializedSize();
    }
  }

  return ofs;
}
