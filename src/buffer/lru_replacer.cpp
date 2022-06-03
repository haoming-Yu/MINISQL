#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : max_size(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

// find the least-recently used element and clear it out.
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // C++17 std::scoped_lock(in <mutex> header) can avoid the deadlock automatically.
  // The ctors can lock automatically on thread level.
  // The dtors will unlock automatically. Therefore keep the thread safe.
  std::scoped_lock lock{mutx_};
  if (LRU_list.empty()) {
    return false;
  }  // if the current buffer frame do not have elements which can be replaced, return false, the victim replacement
     // fails.
  *frame_id = LRU_list.back();  // get the least-recently used element -> lies at the tail of the doubly-linked list
  LRU_hash.erase(*frame_id);    // delete the mapping of the frame_id in the hash table.
  LRU_list.pop_back();          // remove the least-recently used element as a victim from the LRU list.
  return true;
}

// get the specified frame_id out of the buffer frame. -> so that it can not be seen by the LRU algorithm, thus locked
// into the buffer.
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock{mutx_};
  // if can not find the element in the hash table. Nothing to remove from the list. Just return, stop the execution.
  if (LRU_hash.count(frame_id) == 0) {
    return;
  }
  auto iter = LRU_hash[frame_id];
  LRU_list.erase(iter);  // remove the specified element(random access) in the doubly-linked list using a iterator of
                         // the linked-list
  LRU_hash.erase(frame_id);  // remove the corresponding element in the hash table.
}

// put the frame_id into the buffer frame. Or get the pinned element into the LRU algorithm again.
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock{mutx_};
  // case 1: already have this frame_id in the buffer frame.
  // If the hash_table already has this frame_id, return to stop execution directly.
  // avoid the repeated addition of the element (the frame_id of the disk should be unique in the buffer frame.)
  if (LRU_hash.count(frame_id) != 0) {
    return;
  }
  // case 2: already reached the maximum size of buffer frame.
  // Have reached the maximum size of the buffer frame, can not unpin any more, should do the victim replacement first.
  if (LRU_list.size() == max_size) {
    return;
  }
  // case 3: normal case, add the frame_id to the head of the linked list -> most recently used.
  LRU_list.push_front(frame_id);
  LRU_hash.emplace(frame_id, LRU_list.begin());  // refresh the hash table
}

size_t LRUReplacer::Size() { return LRU_list.size(); }  // return the current size of the buffer frame.