#include "catalog/indexes.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t ofs = 0;

  // 1. write the magic number
  MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);
  ofs += sizeof(uint32_t);

  // 2. write the length of index name
  MACH_WRITE_UINT32(buf + ofs, index_name_.size());
  ofs += sizeof(uint32_t);

  // 3. write the string of index_name_
  MACH_WRITE_STRING(buf + ofs, index_name_);
  ofs += index_name_.size();

  // 4. write the index_id_
  MACH_WRITE_UINT32(buf + ofs, index_id_);
  ofs += sizeof(index_id_t);

  // 5. write the table_id_
  MACH_WRITE_UINT32(buf + ofs, table_id_);
  ofs += sizeof(table_id_t);

  // 6. write the size of vector
  MACH_WRITE_UINT32(buf + ofs, key_map_.size());
  ofs += sizeof(uint32_t);

  // 7. write the vector into buf
  for (long unsigned int i = 0; i < key_map_.size(); i++) {
    MACH_WRITE_UINT32(buf + ofs, key_map_[i]);
    ofs += sizeof(uint32_t);
  }

  return ofs;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  if (index_name_.size() == 0) {
    return 0;
  }
  return sizeof(uint32_t) * 5 + index_name_.size() + key_map_.size() * sizeof(uint32_t);
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  if (index_meta != nullptr) {
    std::cerr << "Pointer to index meta data is not null in index meta data deserialization" << std::endl;
  }
  if (buf == nullptr) {
    return 0;
  }

  // 1. Read the magic number
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num == INDEX_METADATA_MAGIC_NUM, "INDEX_METADATA_MAGIC_NUM does not match");
  buf += sizeof(uint32_t);

  // 2. read the length of name
  uint32_t length_name = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  // 3. read the name string out
  std::string name_tmp;
  for (uint32_t i = 0; i < length_name; i++) {
    name_tmp.push_back(buf[i]);
  }
  buf += length_name;

  // 4. read the index_id_
  index_id_t index_tmp = MACH_READ_UINT32(buf);
  buf += sizeof(index_id_t);

  // 5. read the table_id_
  table_id_t table_tmp = MACH_READ_UINT32(buf);
  buf += sizeof(table_id_t);

  // 6. read the size of vector map
  uint32_t map_size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  // 7. read the key_map_ vector out
  std::vector<uint32_t> map_tmp;
  for (uint32_t i = 0; i < map_size; i++) {
    uint32_t tmp = MACH_READ_UINT32(buf);
    map_tmp.push_back(tmp);
    buf += sizeof(uint32_t);
  }

  index_meta = Create(index_tmp, name_tmp, table_tmp, map_tmp, heap);

  return sizeof(uint32_t) * 5 + length_name + map_size * sizeof(uint32_t);
}
