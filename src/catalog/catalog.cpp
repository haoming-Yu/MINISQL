#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  // 1. first serialize the magic number into the buf
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += sizeof(uint32_t);

  // 2. then write the number of pairs in table_meta_pages_ map, (will be used in the deserialization)
  MACH_WRITE_TO(std::size_t, buf, table_meta_pages_.size());
  buf += sizeof(std::size_t);

  // 3. write the pairs of table_meta_page_ into the buf
  for (auto i = table_meta_pages_.begin(); i != table_meta_pages_.end(); ++i) {
    MACH_WRITE_UINT32(buf, i->first);
    buf += sizeof(uint32_t);
    MACH_WRITE_INT32(buf, i->second);
    buf += sizeof(int32_t);
  }

  // 4. write the number of pairs in index_meta_pages
  MACH_WRITE_TO (std::size_t, buf, index_meta_pages_.size());
  buf += sizeof(std::size_t);

  // 5. write the pairs of index_meta_pages into the buf
  for (auto i = index_meta_pages_.begin(); i != index_meta_pages_.end(); ++i) {
    MACH_WRITE_UINT32(buf, i->first);
    buf += sizeof(uint32_t);
    MACH_WRITE_INT32(buf, i->second);
    buf += sizeof(int32_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  if (buf == nullptr) {
    return nullptr; // nothing to deserializefrom, then return the metadata is nullptr
  }
  // 1. first deserialize the magic number
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Catalog_metadata does not match");

  CatalogMeta *rst = NewInstance(heap);  // allocate space for the catalogmeta will be returned

  // 2. then deserialize the size of table_meta_pages
  uint32_t table_num = MACH_READ_FROM (std::size_t, buf);
  buf += sizeof(std::size_t);

  // 3. read the map pairs out
  for (uint32_t i = 0; i < table_num; i++) {
    table_id_t table_id_tmp = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    page_id_t page_id_tmp = MACH_READ_INT32(buf);
    buf += sizeof(int32_t);
    (rst->table_meta_pages_).emplace(table_id_tmp, page_id_tmp);
  }

  // 4. read the number of index_meta_pages' pairs
  uint32_t index_num = MACH_READ_FROM (std::size_t, buf);
  buf += sizeof(std::size_t);

  // 5. read the map pairs out
  for (uint32_t i = 0; i < index_num; i++) {
    index_id_t index_id_tmp = MACH_READ_UINT32(buf);
    buf += sizeof(uint32_t);
    page_id_t page_id_tmp = MACH_READ_INT32(buf);
    buf += sizeof(int32_t);
    (rst->index_meta_pages_).emplace(index_id_tmp, page_id_tmp);
  }

  return rst;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t cnt = 0;
  cnt = sizeof(uint32_t) + sizeof(std::size_t) * 2;
  cnt += table_meta_pages_.size() * (sizeof(uint32_t) + sizeof(int32_t));
  cnt += index_meta_pages_.size() * (sizeof(uint32_t) + sizeof(int32_t));

  return cnt;
}

CatalogMeta::CatalogMeta() {}

CatalogMeta *CatalogManager::GetMeta() { return this->catalog_meta_; }

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  if (init) {
    // the first time catalogManager is created, need to serializa the initial information into the file
    this->catalog_meta_ = CatalogMeta::NewInstance(this->heap_);
  } else {
    // not the first time opening the file, need to deserialize the information from the file
    Page *catalog_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    this->catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_page->GetData(), heap_);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    // load table
    for (auto i : catalog_meta_->table_meta_pages_) {
      Page *iter_page = buffer_pool_manager_->FetchPage(i.second);
      auto *table_info = TableInfo::Create(heap_);
      std::string cur_name;
      TableMetadata *table_meta = nullptr;
      TableMetadata::DeserializeFrom(iter_page->GetData(), table_meta, table_info->GetMemHeap());
      buffer_pool_manager_->UnpinPage(i.second, false);
      if (table_meta != nullptr) { // here the heap does not need to be created for the first time, it has been created when create_table last time, this time just read the information out
        auto *table_heap =
            TableHeap::Create(buffer_pool_manager_, i.second, table_meta->GetSchema(), nullptr, nullptr, table_info->GetMemHeap());
        table_info->Init(table_meta, table_heap);
        cur_name = table_info->GetTableName();
        this->table_names_.emplace(cur_name, i.first);
        this->tables_.emplace(i.first, table_info);
      } else {
        ASSERT(false, "Something wrong caused by deserialization, the table_meta information is null!");
      }
      /* thus table_meta and table_heap are created by table_info */
    }
    // load index
    for (auto i : catalog_meta_->index_meta_pages_) {
      Page *iter_page = buffer_pool_manager_->FetchPage(i.second);
      // Page* iter_page = buffer_pool_manager_->
      auto *index_info = IndexInfo::Create(heap_);
      IndexMetadata *index_meta = nullptr;
      IndexMetadata::DeserializeFrom(iter_page->GetData(), index_meta, index_info->GetMemHeap());
      buffer_pool_manager_->UnpinPage(i.second, false);
      if (index_meta != nullptr) {
        // the information Init function need here can get from the deserialization
        table_id_t cor_table = index_meta->GetTableId();
        // this block is used to find the index's corresponding table, and get its table info
        // //
        //
        auto iter_table = this->catalog_meta_->table_meta_pages_.find(cor_table);
        std::string cor_table_name;
        TableInfo *table_info;
        if (iter_table != this->catalog_meta_->table_meta_pages_.end()) {
          Page *table_page = buffer_pool_manager_->FetchPage(iter_table->second);
          table_info = TableInfo::Create(heap_);
          TableMetadata *table_meta = nullptr;
          TableMetadata::DeserializeFrom(table_page->GetData(), table_meta, table_info->GetMemHeap());
          buffer_pool_manager_->UnpinPage(iter_table->second, false);
          if (table_meta != nullptr) {
            auto *table_heap = TableHeap::Create(buffer_pool_manager_, iter_table->second, table_meta->GetSchema(),
                                                 nullptr, nullptr, table_info->GetMemHeap());
            table_info->Init(table_meta, table_heap);
            cor_table_name = table_info->GetTableName();
          }
        } else {
          ASSERT(false, "Something wrong, can not find the corresponding page information in the CatalogMeta!");
        }
        //
        // // 
        // block end here
        index_info->Init(index_meta, table_info, buffer_pool_manager_);
        std::string index_name = index_info->GetIndexName();
        this->indexes_.emplace(i.first, index_info);    
        auto iter_index_names = index_names_.find(cor_table_name);
        if (iter_index_names == index_names_.end()) {
          // not found in the mapping from table name to unordered_map -> Create a new record
          std::unordered_map<std::string, index_id_t> tmp;
          tmp.emplace(index_name, i.first);
          this->index_names_.emplace(cor_table_name, tmp);
        } else {
          // have found the table name, need to insert an index record into the corresponding mapping container
          (iter_index_names->second).emplace(index_name, i.first); 
          // do not check the replication of record, the create index should do that work, here just a deserialization of information.
        }
      } else {
        ASSERT(false, "Something wrong caused by deserialization, the index_meta information is null!");
      }
    }
  }
}

CatalogManager::~CatalogManager() {
  Page* page = this->buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  this->catalog_meta_->SerializeTo(page->GetData());
  this->buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  auto iter = this->table_names_.find(table_name);
  if (iter != table_names_.end()) {
    // this table name has been occupied
    return DB_TABLE_ALREADY_EXIST;
  }
  // this table is not in the catalog manager right now. Create a new table now.
  page_id_t table_page;
  Page *page = this->buffer_pool_manager_->NewPage(table_page);  // table_page now store the page id of the table
  if (page == nullptr) {
    return DB_FAILED;  // can not new more pages, then return failure, no need to unpin here, the newpage is failed now.
  }
  table_id_t next_table_id_tmp = catalog_meta_->GetNextTableId();
  this->table_names_.emplace(table_name, next_table_id_tmp); // store the map of name and table_id into CatalogManager
  this->catalog_meta_->table_meta_pages_.emplace(next_table_id_tmp, table_page); // store the map of new table information
  auto table_info_tmp = TableInfo::Create(heap_);
  TableMetadata *table_meta = TableMetadata::Create(next_table_id_tmp, table_name, table_page, schema, heap_);
  // after the creation of TableMetadata, need to serialize it into the file storage (non-volatile storage, permanent store)
  table_meta->SerializeTo(page->GetData());
  auto *table_heap =
      TableHeap::Create(buffer_pool_manager_, schema, nullptr, nullptr, nullptr, table_info_tmp->GetMemHeap());
  table_info_tmp->Init(table_meta, table_heap);
  this->tables_.emplace(next_table_id_tmp, table_info_tmp);
  table_info = table_info_tmp;
  this->buffer_pool_manager_->UnpinPage(table_page, true);
  Page *cata_page_refresh = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  this->catalog_meta_->SerializeTo(cata_page_refresh->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true); // after creation of table, do not forget to refresh the meta page storage

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto iter = this->table_names_.find(table_name);
  if (iter == this->table_names_.end()) {
    // the table name is not found
    // if the table is not found, then just do not change the TableInfo information.
    return DB_TABLE_NOT_EXIST;
  }

  // the table name exists
  table_id_t corres_table_id = iter->second;
  auto iter_info = this->tables_.find(corres_table_id);
  if (iter_info == this->tables_.end()) {
    // this recorded name is found, but the recorded TableInfo pairs are not found, so return failure.
    return DB_TABLE_NOT_EXIST;
  }

  // the pairs are found
  table_info = iter_info->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // not called in the entire project, so do not know clearly what this function should do. 
  // Here I just put all the values in the input vector, and return DB_SUCCESS
  // If there are no TableInfo* pairs in the GetTables, return DB_FALIED
  if (tables_.empty()) {
    return DB_FAILED;
  } else {
    for (auto i : tables_) {
      tables.push_back(i.second);
    }
    return DB_SUCCESS;
  }
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  auto iter_table = table_names_.find(table_name);
  // this iterator will be needed when emplace new index record into the table
  if (iter_table == table_names_.end()) {
    return DB_TABLE_NOT_EXIST; // this table is not in the record
  }

  // now find the corresponding table information to check the index keys illegal or not, and do the mapping work if
  // legal
  table_id_t cor_table_id = (this->table_names_.find(table_name))->second;
  auto iter_table_info = this->tables_.find(cor_table_id);
  if (iter_table_info == this->tables_.end()) {
    // can not find the table_id in the list while the name can be found -> error in the storage
    ASSERT(false, "The data is inconsistent, something wrong");
    // this should not happen, the data is not consistent inside the system, something goes wrong
  }
  auto table_index_info = iter_table_info->second;     // get the table info in the list
  Schema *tmp_schema = table_index_info->GetSchema();  // get the schema of the table
  std::vector<uint32_t> tmp_mapping;

  bool is_unique = false;
  for (auto i : index_keys) {
    uint32_t tmp_index;
    if (tmp_schema->GetColumnIndex(i, tmp_index) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    if ((tmp_schema->GetColumn(tmp_index))->IsUnique()) {
      is_unique = true;
    }
    // the column name is found, the column name's index is stored in the tmp_index field
    tmp_mapping.push_back(tmp_index);  // do the mapping work for legal case
  }

  if (!is_unique) {
    // all index_keys are not unique, might cause the B_plus_tree construction failed
    return DB_FAILED; // all the keys are not unique, should not create a key here. Have a probability of failure.
  }

  // try to find the table in the nested mapping, if found, just do the insertion
  index_id_t next_index_id_tmp = catalog_meta_->GetNextIndexId();
  auto iter_nested_table = index_names_.find(table_name);
  if (iter_nested_table == index_names_.end()) {
    // not found
    // then insert an entry into the index_names_, note that all entries in the index_names_ have indexes on it. The
    // logic is synchronized in this way.
    std::unordered_map<std::string, index_id_t> tmp;
    tmp.emplace(index_name, next_index_id_tmp);
    this->index_names_.emplace(table_name, tmp);
  } else {
    auto iter_index = (iter_nested_table->second).find(index_name);
    if (iter_index != (iter_nested_table->second).end()) {
      // find the index name, the index is already in the record
      return DB_INDEX_ALREADY_EXIST;
    }
    // the index name is not in the record, OK to create
    (iter_nested_table->second).emplace(index_name, next_index_id_tmp);
  }
  
  // first create a new page for the index storage
  page_id_t index_page;
  Page *page = this->buffer_pool_manager_->NewPage(index_page);
  if (page == nullptr) {
    return DB_FAILED; // the buffer_pool_manager_ can not new more pages, there are something wrong in the unpin mechanism
    // no need to unpin here, if the returned page is nullptr, the buffer pool manager will not pin any page
  }
  this->catalog_meta_->index_meta_pages_.emplace(next_index_id_tmp, index_page);
  auto *index_info_tmp = IndexInfo::Create(heap_);
  // need to converse the string vector into mapping of uint32_t vector, need to use the table schema, not completed yet
  IndexMetadata *index_meta =
      IndexMetadata::Create(next_index_id_tmp, index_name, cor_table_id, tmp_mapping, heap_);
  // then serialize the creation of meta data into the non-volatile storage
  index_meta->SerializeTo(page->GetData());
  index_info_tmp->Init(index_meta, table_index_info, this->buffer_pool_manager_);
  this->indexes_.emplace(next_index_id_tmp, index_info_tmp);
  auto iter = this->indexes_.find(next_index_id_tmp); // never miss
  index_info = iter->second;
  this->buffer_pool_manager_->UnpinPage(index_page, true); // write the index_meta back into files -> non-volatile storage
  Page *cata_page_refresh = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  this->catalog_meta_->SerializeTo(cata_page_refresh->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto iter_table_name = this->table_names_.find(table_name);
  if (iter_table_name == table_names_.end()) {
    // this table is not found in the table_names_ list
    return DB_TABLE_NOT_EXIST;
  }

  // if this table is found in the table_names_ list, try to find the table names into index_names_
  auto iter_table_name_nested = this->index_names_.find(table_name);
  if (iter_table_name_nested == this->index_names_.end()) {
    // not found in the nested list, the table do not have index created on it. Thus the index is not found
    return DB_INDEX_NOT_FOUND;
  }

  // this table is found in the nested list. It has index created on it, need to further check the index.
  auto iter_index_name = (iter_table_name_nested->second).find(index_name);
  if (iter_index_name == (iter_table_name_nested->second).end()) {
    // this index is not found
    return DB_INDEX_NOT_FOUND;
  }
  // this index is found, find the corresponding IndexInfo pointer
  index_id_t search_idx = iter_index_name->second;
  auto iter_index_info = this->indexes_.find(search_idx);
  if (iter_index_info == indexes_.end()) {
    return DB_FAILED; // two map is not consistent with each other, something goes error
  }

  index_info = iter_index_info->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // MAKE SURE YOUR INDEXES IS INITIALLY EMPTY, or the result will be pushed right after the tail element in the indexes vector
  // not used in inner implementation, but might be used by the caller
  // this function will put all the IndexInfo information of the table_name into the vector
  // So make sure that your indexes is initially EMPTY !!!
  auto iter_table = this->table_names_.find(table_name);
  if (iter_table == table_names_.end()) {
    // the table_name is not found
    return DB_TABLE_NOT_EXIST;
  }

  auto iter_table_nested = (this->index_names_).find(table_name);
  if (iter_table_nested == index_names_.end()) {
    return DB_INDEX_NOT_FOUND; // no indexes created on the table_name table
  }

  // the table_name is found, now traverse the map <index_name, index_id_t>
  for (auto iter_index = (iter_table_nested->second).begin(); iter_index != (iter_table_nested->second).end(); ++iter_index) {
    auto search_info = this->indexes_.find(iter_index->second);
    if (search_info == this->indexes_.end()) {
      return DB_FAILED; // inconsistence in the DB system!
    }
    indexes.push_back(search_info->second);
  }

  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  // 1. first need to check whether this table is created before.
  auto iter_table_names = this->table_names_.find(table_name);
  if (iter_table_names == this->table_names_.end()) {
    // the table is not found
    return DB_TABLE_NOT_EXIST;
  }

  // Get the information about the table
  table_id_t tbl_id = iter_table_names->second;
  auto iter_meta = this->catalog_meta_->table_meta_pages_.find(tbl_id);
  if (iter_meta == this->catalog_meta_->table_meta_pages_.end()) {
    return DB_FAILED; // this is not found, the database record is inconsistent
  }
  page_id_t page_id = iter_meta->second;
  auto iter_tableIFO = this->tables_.find(tbl_id);
  if (iter_tableIFO == this->tables_.end()) {
    return DB_FAILED; // inconsistence in the record of DB
  }
  TableInfo *IFO = iter_tableIFO->second;
  TableHeap *tbl_heap = IFO->GetTableHeap();

  // 2. check whether this table has some indexes on it
  auto iter_index_names = this->index_names_.find(table_name);
  if (iter_index_names == this->index_names_.end()) {
    // the table is found and there are no indexes created on it.
    // just drop the table, and clear the file storage
    tbl_heap->FreeHeap(); // clear the table heap storage
    this->buffer_pool_manager_->DeletePage(page_id); // clear the page of the table information (table meta actually)
    this->catalog_meta_->table_meta_pages_.erase(tbl_id);
    this->table_names_.erase(table_name);
    this->tables_.erase(tbl_id);
    return DB_SUCCESS;
  }

  // the table has indexes on it, also need to delete the indexes (first delete the index, then delete the table) <- mysql
  for (auto iter = (iter_index_names->second).begin(); iter != (iter_index_names->second).end(); ++iter) {
    // traverse every index created on the table, and clear away all the indexes
    if (this->DropIndex(table_name, iter->first, false) == DB_FAILED) {
      return DB_FAILED; // some index can not be dropped, the table can not be dropped away as well.
    }
  }
  // all the table's indexes have been deleted away, just clear the nested mapping by the caller
  (iter_index_names->second).clear();
  this->index_names_.erase(table_name);
  
  // now do the clear of table
  // tbl_heap->FreeHeap(); // <- still problem, maybe do not need to delete the heap here, the tuples are all in the indexes.
  this->buffer_pool_manager_->DeletePage(page_id);
  this->catalog_meta_->table_meta_pages_.erase(tbl_id);
  this->table_names_.erase(table_name);
  this->tables_.erase(tbl_id);
  return DB_SUCCESS;
}

// note that the caller must need to judge whether there is no more indexes left on some tables
// if no more, do not forget to clear the nested mapping away.
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name, bool refresh) {
  // Note that we should call the b_plus_tree's destroy, the destroy function will update the index_roots_page (check again)
  // call buffer_pool_manager_'s deletepage to delete corresponding pages.
  auto iter_table_names = this->table_names_.find(table_name);
  if (iter_table_names == this->table_names_.end()) {
    return DB_TABLE_NOT_EXIST; // the table does not exist in the database.
  }

  auto iter_index_names = this->index_names_.find(table_name);
  if (iter_index_names == this->index_names_.end()) {
    // this index's host table is not found, which means that this table do not have any indexes created on it.
    return DB_INDEX_NOT_FOUND;
  }

  auto iter_nested = (iter_index_names->second).find(index_name);
  if (iter_nested == (iter_index_names->second).end()) {
    return DB_INDEX_NOT_FOUND;
  }
  // else the index is found
  index_id_t idx_id = iter_nested->second;
  // 1. first refresh the record in the object of catalog class, and find the actual page id at the same time
  // the page id must have been NewPage before, so just call the FetchPage will be OK.
  auto iter_meta = this->catalog_meta_->index_meta_pages_.find(idx_id);
  if (iter_meta == this->catalog_meta_->index_meta_pages_.end()) {
    return DB_FAILED; // inconsistence in DB_record, something goes wrong in the module 4 -> catalog manager
  }

  // 2. then find the index's corresponding B_Plus_tree and destroy it.
  auto iter_indexIFO = this->indexes_.find(idx_id);
  if (iter_indexIFO == this->indexes_.end()) {
    return DB_FAILED; // inconsistence in DB_record, something goes wrong in the drop index
  }

  page_id_t page_id = iter_meta->second; // the page of the index meta;
  IndexInfo *IFO = iter_indexIFO->second; // the indexinfo of the index;

  // 3. try to clear the B_plus tree.
  if (IFO->GetIndex()->Destroy() == DB_FAILED) {
    // the deletion of B_plus tree is failed, something goes wrong. The usage of B_PLUS_TREE destroy might have problem
    return DB_FAILED;
  }

  // 4. clear the index meta information, delete the actual record in the file of the index_meta page
  if (!(this->buffer_pool_manager_->DeletePage(page_id))) {
    return DB_FAILED; // the deletion of page is failed.
  }

  // 5. clear the record in the catalog manager, do this thing at last to avoid the inconsistence of record.
  if (refresh) {
    (iter_index_names->second).erase(index_name);  // clear the record of index_name away
    // the caller will not do the refresh of nested mapping itself
    if ((iter_index_names->second).size() == 0) {
      // the table has no more index on it, clear the table name away as well
      this->index_names_.erase(table_name);
    }
  }
  // if refresh is false, it means that the caller do not need the drop index function to do the refreshing of nested mapping
  
  // already get the information in this map, so erase the record away.
  this->catalog_meta_->index_meta_pages_.erase(iter_meta->first);
  // clear the indexINFO mapping away
  this->indexes_.erase(idx_id);

  // 6. free the space of indexIFO, which will be done automatically by the heap.

  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // do the flush work for CatalogMetapage, private function, but not used in my implementation
  // just do the flush work, do not do the unpin work automatically, need the caller to care about this information.
  bool fs_rst = this->buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  if (fs_rst) {
    return DB_SUCCESS;
  } else {
    return DB_FAILED;
  }
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  ASSERT(false, "Not Implemented yet"); // Not used in my implementation -> directly implement it in the ctor of the class
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  ASSERT(false, "Not Implemented yet"); // Not used in my implementation -> directly implement it in the ctor of the class
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto iter = tables_.find(table_id);
  if (iter == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  } else {
    table_info = iter->second;
    return DB_SUCCESS;
  }
}