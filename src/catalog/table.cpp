#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  // serialize the information into buf array, return the offset of buf array caused by me
  uint32_t ofs = 0; // record the total offset returned

  // 1. first write in the magic number for recognization of TableMetaData
  MACH_WRITE_UINT32(buf + ofs, TABLE_METADATA_MAGIC_NUM);
  ofs += sizeof(uint32_t);

  // 2. then write in the table_id_
  MACH_WRITE_UINT32(buf + ofs, table_id_);
  ofs += sizeof(table_id_);

  // 3. then write the length of the table_name_
  MACH_WRITE_UINT32(buf + ofs, table_name_.size());
  ofs += sizeof(uint32_t);

  // 4. then write in the table_name_
  MACH_WRITE_STRING(buf + ofs, table_name_);
  ofs += table_name_.size();

  // 5. then write in the root_page_id_
  MACH_WRITE_INT32(buf + ofs, root_page_id_);
  ofs += sizeof(root_page_id_);

  // 6. then write in the schema pointer
  MACH_WRITE_TO(Schema *, buf + ofs, schema_);
  ofs += sizeof(schema_);

  return ofs;
}

uint32_t TableMetadata::GetSerializedSize() const {
  if (table_name_.length() == 0) {
    return 0;
  }
  return sizeof(schema_) + sizeof(root_page_id_) + table_name_.size() + sizeof(table_id_) + sizeof(uint32_t) * 2;
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  if (table_meta != nullptr) {
    std::cerr << "Pointer to column is not null in table meta data deserialize." << std::endl;
  }
  if (buf == nullptr) {
    return 0;
  }

  // 1. read and check the magic number
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num == TABLE_METADATA_MAGIC_NUM, "TABLE_META_DATA_MAGIC_NUM does not match");
  buf += sizeof(uint32_t);

  // 2. read the table_id_
  table_id_t table_id_tmp = MACH_READ_UINT32(buf);
  buf += sizeof(table_id_t);

  // 3. read the length of table_name_
  uint32_t length = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  // 4. read the string out
  std::string name_tmp;
  for (uint32_t i = 0; i < length; i++) {
    name_tmp.push_back(buf[i]);
  }
  buf += length;

  // 5. read the root_page_id_
  page_id_t root_id_tmp = MACH_READ_INT32(buf);
  buf += sizeof(page_id_t);

  // 6. read the schema pointer out
  Schema *schema_tmp = *reinterpret_cast<Schema **>(buf);
  buf += sizeof(Schema*);
  
  table_meta = Create(table_id_tmp, name_tmp, root_id_tmp, schema_tmp, heap);

  return sizeof(schema_) + sizeof(root_page_id_) + length + sizeof(table_id_) + sizeof(uint32_t) * 2;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
