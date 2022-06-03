#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

// ctors have already been given
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    // initial state: all the pages in the buffer frame is free.
    // push them all into the free list.
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    // flush all the memory pages into the physical storage(disk)
    FlushPage(page.first);  // page is a tuple <page_id_t, frame_id_t> -> page.first get the page_id_t of the mapping.
  }
  delete[] pages_;
  delete replacer_;  // call the dtor function of the object replacer_ pointing to.
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock{latch_};
  auto search_page = page_table_.find(page_id);
  if (search_page != page_table_.end()) {
    // this page exists in the page_table_ (this page is in the buffer pool)
    frame_id_t frame_id = search_page->second;
    Page *page = &(pages_[frame_id]);  // get the specified page in the buffer pool
    replacer_->Pin(frame_id);          // pin it, so it can not be replaced by the LRU algorithm
    page->pin_count_++;                // after the pin of LRU replacer, need to refresh the pin_count_ label.
    return page;                       // return the page fetched from the memory to the executor.
  } else {                             // this page is not in the buffer pool, in the disk
    frame_id_t frame_id = -1;
    // use the self-defined function find_victim_page to find the victim page from 2 case
    // -> free_list_ or LRU replacer's advice.
    if (!find_victim_page(&frame_id)) {  // no replacement solution, fetching fails.
      return nullptr;
    }
    // the victim page has been found, now replace the data with the page's content.
    Page *page = &(pages_[frame_id]);
    update_page(page, page_id, frame_id);
    // clear the data to be zero. If the page is dirty, write it into disk, and then set dirty to be false. Clear the
    // data to be zero as well.
    disk_manager_->ReadPage(page_id, page->data_);  // read the database file (page_id position) to new page->data
    replacer_->Pin(frame_id);                       // pin the new data read in
    page->pin_count_ = 1;  // "++" is OK, but here is equal to create a page, so "= 1" is better. Cause this page is
                           // loaded from the disk(new to memory), therefore, just set the pin_count to be 1.
    // actually, every time we read a disk page into memory, only the data_ will be read, the other infomations are
    // created again. is_Dirty_ is defaultly set to be false, here no need to change it. -> The page is already Pin
    return page;
  }
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock{latch_};
  frame_id_t frame_id = -1;
  // case 1: can not get victim frame_id, the new page operation fails.
  if (!find_victim_page(&frame_id)) {
    return nullptr;
  }
  // case 2: got victim frame_id
  page_id = AllocatePage();          // allocate a new disk page_id, change the argument page_id.
  Page *page = &(pages_[frame_id]);  // get buffer pool page from the frame_id
  page->pin_count_ = 1;  // set the pin_count_ to be 1 when a new disk page is loaded into memory buffer pool.f
  update_page(page, page_id,
              frame_id);     // update the page content to be the disk_page -> page_id, and buffer pool_page -> frame_id
  replacer_->Pin(frame_id);  // pin the new updated frame_id when a new disk page has just been put into the memory
 

  return page;
}

// here the page_id is the disk page id -> also the key of hash table
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock{latch_};
  auto search = page_table_.find(page_id);
  // case 1: the page does not exist, just return true.
  if (search == page_table_.end()) {
    return true;
  }
  // case 2: normal case, the page exists in the buffer pool
  frame_id_t frame_id = search->second;
  Page *page = &(pages_[frame_id]);
  // case 2.1: the page is still used by some thread, can not delete
  if (page->pin_count_ > 0) {
    return false;
  }
  // case 2.2: pin_count_ == 0, can be deleted
  /*if (page_id == 3) {
    static int i = 1;
    cout << "fuck " << i << " times" << endl;
    i++;
  }*/
  DeallocatePage(page_id);                       // deallocate the corresponding disk file
  update_page(page, INVALID_PAGE_ID, frame_id);  // set the page's disk page to INVALID value.
  free_list_.push_back(frame_id);                // add the free frame page to tail of the free list.

  return true;
}

// the page_id argument is the disk page id.
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};
  auto search = page_table_.find(page_id);
  // case 1: the page doesn't exist in the buffer
  if (search == page_table_.end()) {
    return false;
  }

  // case 2: the page exist in the page table
  frame_id_t frame_id = search->second;
  Page *page = &(pages_[frame_id]);
  // case 2.1: if the pin_count_ = 0; -> the page has not been pinned before
  if (page->pin_count_ == 0) {
    return false;
  }

  // case 2.2: the pin_count_ > 0
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    // only when the pin_count_ has reduced to 0, can the replacer do the unpin operation!
    // or some unpin operation will fail because the directly unpin of replacer.
    // must ensure all the thread and pins work until they are all unpinned. When a page is unpinned in the replacer, it
    // might be deleted from the buffer pool
    replacer_->Unpin(frame_id);
  }
  if (is_dirty) {
    page->is_dirty_ = true;  // if the unpinned page is now dirty, then change the page infomation about this page
    // if the pinned page is not dirty now, do not change it, because it might be dirty originally.
    // this is not equal to: page->is_dirty_ = is_dirty
  }

  return true;
}

void BufferPoolManager::update_page(Page *page, page_id_t new_page_id, frame_id_t new_frame_id) {
  // step 1: if it is dirty -> write it back to disk, and set dirty to false.
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }

  // step 2: refresh the page table
  page_table_.erase(page->page_id_);     // delete the page_id and its frame_id in the original page_table_
  if (new_page_id != INVALID_PAGE_ID) {  // the object contains a physical page. If INVALID_PAGE_ID, then do not add it
                                         // to the page_table_
    page_table_.emplace(new_page_id, new_frame_id);  // add new page_id and the corresponding frame_id into page_table_
  }

  // step 3: reset the data in the page(clear out it to be zero), and page id
  page->ResetMemory();
  page->page_id_ = new_page_id;
}

bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {
  // case 1: the buffer pool is not full, the free_list still have elements
  //         in this case, just get one out of the free_list -> get frame_id from the head of free_list.
  // (in DeletePage function, we add frame_id at the tail of free_list)
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  // case 2: the buffer pool is already full, need to call LRU replacer.
  return replacer_->Victim(frame_id);
}

// flush the correspondence page into disk, return the operation state.
// This function only does a refreshing mechanism in disk. Do not change the memory contents actually
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::scoped_lock lock{latch_};
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto search = page_table_.find(page_id);  // search is the pointer(iterator) to the mapping tuple.
  if (search != page_table_.end()) {
    // found the corresponding frame page in memory.
    frame_id_t frame_id = search->second;
    Page *page = &(pages_[frame_id]);
    disk_manager_->WritePage(page->page_id_, page->data_);  // here the page_id_ labels the disk page_id
    page->is_dirty_ = false;  // When we've flush the page into the disk, we need to set dirty label to be false.
  } else {
    // the required physical page does have corresponding frame page in the buffer memory.
    return false;
  }

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock lock{latch_};
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page = &(pages_[i]);
    if (page->page_id_ != INVALID_PAGE_ID && page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    /**
     * another way to implement this part:
     * FlushPage(page->page_id_);
     */
  }
}

// already implement this function in the disk_manager module.
// @return value -> the disk page id of next page
page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  //for (size_t i = 0; i < 100; i++) {
  //
  //    
  //    //LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
  //    cout << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
  //  
  //}
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
      cout << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}