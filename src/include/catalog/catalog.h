#ifndef MINISQL_CATALOG_H
#define MINISQL_CATALOG_H

#include <string>
#include <map> // map -> use red-black tree to store the pair
#include <unordered_map> // unordered_map -> use hash map to store the pair

#include "buffer/buffer_pool_manager.h"
#include "catalog/indexes.h"
#include "catalog/table.h"
#include "common/config.h"
#include "common/dberr.h"
#include "transaction/lock_manager.h"
#include "transaction/log_manager.h"
#include "transaction/transaction.h"

class CatalogMeta {
  friend class CatalogManager;

public:
  void SerializeTo(char *buf) const;

  static CatalogMeta *DeserializeFrom(char *buf, MemHeap *heap);

  uint32_t GetSerializedSize() const;

  inline table_id_t GetNextTableId() const {
    // this can get the next table id for the create table function
    return table_meta_pages_.size() == 0 ? 0 : table_meta_pages_.rbegin()->first + 1;
  }

  inline index_id_t GetNextIndexId() const {
    return index_meta_pages_.size() == 0 ? 0 : index_meta_pages_.rbegin()->first + 1;
  }

  static CatalogMeta *NewInstance(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(CatalogMeta));
    return new(buf) CatalogMeta();
  }

  /**
   * Used only for testing
   */
  inline std::map<table_id_t, page_id_t> *GetTableMetaPages() {
    return &table_meta_pages_;
  }

  /**
   * Used only for testing
   */
  inline std::map<index_id_t, page_id_t> *GetIndexMetaPages() {
    return &index_meta_pages_;
  }

private:
  explicit CatalogMeta();

private:
  static constexpr uint32_t CATALOG_METADATA_MAGIC_NUM = 89849;
  std::map<table_id_t, page_id_t> table_meta_pages_;
  std::map<index_id_t, page_id_t> index_meta_pages_;
};

/**
 * Catalog manager
 *
 */
class CatalogManager {
public:
  explicit CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                          LogManager *log_manager, bool init);

  ~CatalogManager();

  dberr_t CreateTable(const std::string &table_name, TableSchema *schema, Transaction *txn, TableInfo *&table_info);

  dberr_t GetTable(const std::string &table_name, TableInfo *&table_info);

  dberr_t GetTables(std::vector<TableInfo *> &tables) const;

  dberr_t CreateIndex(const std::string &table_name, const std::string &index_name,
                      const std::vector<std::string> &index_keys, Transaction *txn,
                      IndexInfo *&index_info);

  dberr_t GetIndex(const std::string &table_name, const std::string &index_name, IndexInfo *&index_info) const;

  dberr_t GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const;

  dberr_t DropTable(const std::string &table_name);

  dberr_t DropIndex(const std::string &table_name, const std::string &index_name, bool refresh);
  // if refresh is true, it means that the caller need the DropIndex function to do the refreshing of nested mapping
  // else if refresh is false, it means that the DropIndex function do not do the refreshing, the caller will do it later.
  // do not interference normal usage, just set refresh to be true when you need to drop index normally as before
  MemHeap *GetMemHeap() { return heap_; }
  dberr_t GetAllTableNames(std::vector<std::string> &table_names ) {
    for (auto i : this->table_names_) {
      table_names.push_back(i.first);
    }
    return DB_SUCCESS;
  }

  dberr_t GetAllIndexNames(const std::string &table_name, std::vector<std::string> &indexes) {
    auto iter_outer = this->index_names_.find(table_name);
    if (iter_outer == this->index_names_.end()) {
      return DB_INDEX_NOT_FOUND;
    }
    for (auto i : iter_outer->second) {
      indexes.push_back(i.first);
    }
    return DB_SUCCESS;
  }

  // for convenience
  CatalogMeta *GetMeta(void);

private:
  dberr_t FlushCatalogMetaPage() const;

  dberr_t LoadTable(const table_id_t table_id, const page_id_t page_id);

  dberr_t LoadIndex(const index_id_t index_id, const page_id_t page_id);

  dberr_t GetTable(const table_id_t table_id, TableInfo *&table_info);

private:
  BufferPoolManager *buffer_pool_manager_;
  LockManager *lock_manager_;
  LogManager *log_manager_;
  CatalogMeta *catalog_meta_; // need to deal with this members serialization
  [[maybe_unused]] std::atomic<table_id_t> next_table_id_; // not used in my implementation
  [[maybe_unused]] std::atomic<index_id_t> next_index_id_; // not used in my implementation
  // map for tables
  std::unordered_map<std::string, table_id_t> table_names_;
  std::unordered_map<table_id_t, TableInfo *> tables_;
  // map for indexes: table_name->index_name->indexes
  // the mapping relationship: a table_name might have several index_name mapped to it, but one index_name can only have one index_id_t mapping
  std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>> index_names_;
  // table_name --mapping--> unordered_map: one to one; index_name --mapping--> index_id_t: one to one; but there are a lot of elements in the <index_name, index_id_t> mapping container.
  std::unordered_map<index_id_t, IndexInfo *> indexes_;
  // memory heap
  MemHeap *heap_;
};

#endif //MINISQL_CATALOG_H
