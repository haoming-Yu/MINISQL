#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 explicit IndexIterator(page_id_t CurrPageId, int CurrPosition, BufferPoolManager *bufferPoolManager);

  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
  // add your own private member variables here
	BufferPoolManager *bufferPoolManager;
		page_id_t CurrPageId;
        int CurrLocation;
		// if we Read the Tuple from the Leaf Page , We need to Copy the Item
        MappingType *Item = nullptr;

};


#endif //MINISQL_INDEX_ITERATOR_H
