#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(page_id_t CurrPageId,int CurrPosition,BufferPoolManager * bufferPoolManager) {
  this->CurrPageId = CurrPageId;
  this->CurrLocation = CurrPosition;
  this->bufferPoolManager = bufferPoolManager;
  this->Item = nullptr;
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if (this->Item != nullptr) delete this->Item;
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {

  auto page = bufferPoolManager->FetchPage(this->CurrPageId);
  auto node = reinterpret_cast<LeafPage *>(page);
  // Free the preview Item
  if (this->Item != nullptr) delete this->Item;
  this->Item = new MappingType;
  MappingType *Pair = new (this->Item) MappingType(node->GetItem(this->CurrLocation));
  bufferPoolManager->UnpinPage(this->CurrPageId,false);
  return *Pair;
  
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
   auto page = bufferPoolManager->FetchPage(this->CurrPageId);
   auto node = reinterpret_cast<LeafPage *>(page);
   int Capacity = node->GetSize();
  // just Point to Next Pair in this Page
   if (this->CurrLocation + 1 < Capacity) {
     bufferPoolManager->UnpinPage(this->CurrPageId, false);
     CurrLocation++;
   } else {
     page_id_t NextPage = node->GetNextPageId();
    
     bufferPoolManager->UnpinPage(this->CurrPageId,false);
     // It means NextPage is the Last Page
     if (NextPage == INVALID_PAGE_ID) {
       this->CurrPageId = INVALID_PAGE_ID;
       this->CurrLocation = 0;
     } else {
       // Update Next Page
       this->CurrPageId = NextPage;
       // Update Next Position
       this->CurrLocation = 0;
     }
     
   }
   return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if (this->CurrLocation == itr.CurrLocation && this->CurrPageId == itr.CurrPageId) {
    return true;
  } else
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const { 
    
    return !(*this == itr);
 
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
