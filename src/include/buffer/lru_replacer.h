#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>  // doubly-linked list implementation in STL, once an element is visited, it should be put at the head(most-recently used); The least-recently used element is always at the tail of the list.
#include <mutex>
#include <unordered_map>  // hash table for mapping the corresponding frame_id in the doubly-linked list, quickly find whether an element is in the list
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // add your own private member variables here
  std::mutex mutx_;                // lock for threads
  std::list<frame_id_t> LRU_list;  // doubly-linked list for storage of frame_id_t -> implementation of least-recently
                                   // used (LRU algorithm)
  std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> LRU_hash;
  // hash table, for frame_id_t storage (first find the corresponding key, frame_id_t(hash version) ---map--->
  // linked_list's iterator) which can be accessed by using frame_id_t as a index, and the content is the iterator of
  // std::list -> which can be used to directly modify the linked-list's element and structure.
  size_t max_size;
};

#endif  // MINISQL_LRU_REPLACER_H
