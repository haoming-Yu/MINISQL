#include "record/column.h"
#include <iostream>
using namespace std;
//Support int or Float -> does not ha a length with its ctor (the length is 0 by default.)
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) { // here use std::move to make copy more efficient
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type."); // prevent upper level's wrong using and wrong call
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}
//Support for the Char Type -> has length with its ctor. Here length means the maximum storage space for character array.
Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  uint32_t ofs=0;

  //1. Write the MagicNum
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  ofs += sizeof(uint32_t);

  //2. Write the length for the Name
  MACH_WRITE_UINT32(buf+ofs, this->name_.length());
  ofs += sizeof(uint32_t);

  //*3. Write the string name to the buf (-> this is a string)
  MACH_WRITE_STRING(buf+ofs, this->name_);
  ofs += this->name_.length();

  //4. Write the type_ to the buf
  MACH_WRITE_TO(TypeId, (buf+ofs), (this->type_));
  ofs += sizeof(TypeId);

  //5. Write the len_ to the buf
  MACH_WRITE_UINT32(buf+ofs, this->len_);
  ofs += sizeof(uint32_t);

  //6. Write the table_ind_
  MACH_WRITE_UINT32(buf+ofs, this->table_ind_);
  ofs += sizeof(uint32_t);

  //7. Write the nullable_ to the buf
  MACH_WRITE_BOOL(buf+ofs, this->nullable_);
  ofs += sizeof(bool);

  //8. Write the unique_ to the buf
  MACH_WRITE_BOOL(buf + ofs, this->unique_);
  ofs += sizeof(bool);

  return ofs;
}

uint32_t Column::GetSerializedSize() const { // calculate the serializedSize of column, maybe used in the upper level estimation.
  uint32_t ofs=0;
  if(this->name_.length()==0)
  {
    return 0;
    // The Column does not have a name, which means that the column does not exist actually. 
    // -> this require the upper level calling to this function must keep the rule that the attribute must have a name.
  }
  else
  {
    ofs = sizeof(uint32_t) * 4 + sizeof(bool) * 2 + sizeof(TypeId);
    ofs += this->name_.length();
  }
  return ofs;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  if (column != nullptr) {
    // std::cerr << "Pointer to column is not null in column deserialize." << std::endl;
  } // a warning of covering original column storage in memory. -> Maybe need to comment away for upper level transparent.
  if(buf==NULL) return 0; // nothing to deserialize from. 
  
  /* deserialize field from buf */

  //1.Read the Magic_Number
  uint32_t Magic_Number = MACH_READ_UINT32(buf);
  ASSERT(Magic_Number == 210928, "COLUMN_MAGIC_NUM does not match"); // check using assert. -> This will automatically stop the program.
  /** A equivalent expression of using ASSERT macro. This is going to report a wrong message, but keep running the caller routine.
  if(Magic_Number!=210928)
  {
    std::cerr<<"COLUMN_MAGIC_NUM does not match"<<endl;
    return 0;
  }
  */
  buf += sizeof(uint32_t); // refresh buf to another member storage.

  //2.Read the length of the name_
  uint32_t length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  //3.Read the Name from the buf
  std::string column_name;
  for(uint32_t i=0;i < length;i++)
  {
    column_name.push_back(buf[i]);
  }
  buf += length; // the storage of string is compact, so just add the length is OK.

  //4.Read the type
  TypeId type=MACH_READ_FROM(TypeId, (buf));
  buf += sizeof(TypeId);

  //5.Read the len_
  uint32_t len_ = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  //6.Read the col_ind
  uint32_t col_ind = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  //7.Read the nullable
  bool nullable=MACH_READ_FROM(bool,(buf));
  buf += sizeof(bool);

  //8.Read the unique
  bool unique=MACH_READ_FROM(bool,(buf));
  buf += sizeof(bool);

  // can be replaced by: 
  //		ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  
  void *mem = heap->Allocate(sizeof(Column));
  if (type == kTypeInt || type == kTypeFloat) {
    // type is the int or float
    column = new (mem) Column(column_name, type, col_ind, nullable, unique);
  } else if (type == kTypeChar) {
    column = new (mem) Column(column_name, type, len_, col_ind, nullable, unique);
  }
  
  return sizeof(uint32_t) * 4 + sizeof(bool) * 2 + sizeof(TypeId) + length;
}
