#ifndef MINISQL_BUFFER_POOL_MANAGER_H
#define MINISQL_BUFFER_POOL_MANAGER_H

#include <list>
#include <mutex>
#include <unordered_map>

#include "buffer/lru_replacer.h"
#include "page/disk_file_meta_page.h"
#include "page/page.h"
#include "storage/disk_manager.h"

using namespace std;  // only effective in this file scope, outside the cpp file is not effective.

class BufferPoolManager {
 public:
  explicit BufferPoolManager(size_t pool_size, DiskManager *disk_manager);

  ~BufferPoolManager();

  Page *FetchPage(page_id_t page_id);

  bool UnpinPage(page_id_t page_id, bool is_dirty);

  bool FlushPage(page_id_t page_id);

  void FlushAllPages(void);  // this function is added by myself

  Page *NewPage(page_id_t &page_id);

  bool DeletePage(page_id_t page_id);

  bool IsPageFree(page_id_t page_id);

  bool CheckAllUnpinned();

 private:
  /**
   * Allocate new page (operations like create index/table) For now just keep an increasing counter
   */
  page_id_t AllocatePage();

  /**
   * Deallocate page (operations like drop index/table) Need bitmap in header page for tracking pages
   */
  void DeallocatePage(page_id_t page_id);

  /**
   * return state of the operation, and the frame_id (by pointer argument) from free_list or replacer
   * This function is added by myself
   */
  bool find_victim_page(frame_id_t *frame_id);

  /**
   * Write the dirty page into disk, and refresh the meta data of page (data, is_dirty, page_id) and page_table
   * This function is added by myself.
   */
  void update_page(Page *page, page_id_t new_page_id, frame_id_t new_frame_id);

 private:
 
  size_t pool_size_;           // number of pages in buffer pool
  Page *pages_;                // array of pages
  DiskManager *disk_manager_;  // pointer to the disk manager.
  std::unordered_map<page_id_t, frame_id_t>
      page_table_;  // to keep track of pages -> using HASH table to record the mapping between frame_id_t(in-memory)
                    // and page_id_t(on-disk)
  Replacer *replacer_;  // to find an unpinned page for replacement -> Here using the LRU replacer.
  std::list<frame_id_t>
      free_list_;          // to find a free page for replacement -> A doubly-linked list recording the free page.
  recursive_mutex latch_;  // to protect shared data structure -> a lock on thread level.
};

#endif  // MINISQL_BUFFER_POOL_MANAGER_H