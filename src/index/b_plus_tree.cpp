#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  IndexRootsPage *idx_roots_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  bool ret = idx_roots_page->GetRootId(index_id_, &root_page_id_);
  if (ret == false) {
    root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  //BFS- 
  //Get the Root Page
  if (this->root_page_id_ == INVALID_PAGE_ID) return;
  auto page = buffer_pool_manager_->FetchPage(this->root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  buffer_pool_manager_->UnpinPage(this->root_page_id_,false);

  queue<page_id_t> AllPage;
  AllPage.push(node->GetPageId());
  while (AllPage.empty() == false) {
    page_id_t CurrentPage = AllPage.front();
    AllPage.pop();
    auto page = buffer_pool_manager_->FetchPage(CurrentPage);
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // it means Current Page Is not the Leaf Page
    if (node->IsLeafPage() == false) {
      //Enqueue the ChildPage of the Current Page
      auto node = reinterpret_cast<InternalPage *>(page->GetData());
      for (int i = 0; i < node->GetSize(); i++) {
        AllPage.push(node->ValueAt(i));
      }
      buffer_pool_manager_->UnpinPage(CurrentPage, false);
      node->SetSize(0);
      bool state=buffer_pool_manager_->DeletePage(CurrentPage);
      if (state == false) {
        throw " BPLUSTREE_TYPE::Destroy()-Failed";
      }
    } else {
      // it means Current Page is the Leaf Page We need to Pop all the Leaf Page
      buffer_pool_manager_->UnpinPage(CurrentPage, false);
      auto LeafPage2 = buffer_pool_manager_->FetchPage(CurrentPage);
      auto LeafNode2 = reinterpret_cast<LeafPage *>(LeafPage2->GetData());
      LeafNode2->SetSize(0);
      buffer_pool_manager_->UnpinPage(CurrentPage, true);
        
      bool state = buffer_pool_manager_->DeletePage(CurrentPage);
      if (state == false) {
        throw " BPLUSTREE_TYPE::Destroy()-Failed";
      }
        //Clear the All Leaf Node
      while (AllPage.empty() == false) {
        page_id_t LeafPageId=AllPage.front();
        AllPage.pop();
        auto LeafPage1 = buffer_pool_manager_->FetchPage(LeafPageId);
        auto LeafNode1 = reinterpret_cast<LeafPage *>(LeafPage1->GetData());
        LeafNode1->SetSize(0);
        buffer_pool_manager_->UnpinPage(LeafPageId, true);

        bool state = buffer_pool_manager_->DeletePage(LeafPageId);
        if (state == false) {
          throw " BPLUSTREE_TYPE::Destroy()--LeafPage-Failed";
        }
      }
    }
  }
  this->root_page_id_ = INVALID_PAGE_ID;
  this->UpdateRootPageId(false);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return this->root_page_id_==INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  bool state = false;
  auto page = this->FindLeafPage(key, false);
  auto* leaf = reinterpret_cast<LeafPage *> (page->GetData());
  ValueType tmp;
  if (leaf->Lookup(key,tmp,this->comparator_) == true) {
      //key is exist in the LeafPage
    result.push_back(tmp);
    state = true;
   } 
  else 
  {
    //Key is exist or not
    state = false;
   }
   buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
   return state;
}


/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) { 
    bool state = false;
    //Empty Tree
    if (this->IsEmpty()) {
      //std::cout << "Insert::StartNewTree" << endl;
      this->StartNewTree(key, value);
      state = true;
    } else {
      //std::cout << "Insert::InsertIntoLeaf" << endl;
      state=InsertIntoLeaf(key, value, transaction);
    }
    return state;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
    page_id_t NewId = INVALID_PAGE_ID;
    //Get the New Root
    Page *page = buffer_pool_manager_->NewPage(NewId);
    if (page != nullptr) {
      auto *node = reinterpret_cast<LeafPage *>(page->GetData());
      //Init Leaf Root
      node->Init(NewId, INVALID_PAGE_ID, this->leaf_max_size_);
      //Insert First Tuple
      node->Insert(key, value, this->comparator_);
      //Set the Root Page to the NewId
      this->root_page_id_ = NewId;
      this->UpdateRootPageId(true);
      buffer_pool_manager_->UnpinPage(NewId,true);
    } else {
      buffer_pool_manager_->UnpinPage(NewId, false);
      std::cerr << "out of memory" << endl;
    }

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
   
    //1. Find the Leaf Node for the key
  bool state = false;
   //std::cout << "InsertIntoLeaf::FindLeafPage()" << endl;
  auto page = FindLeafPage(key, false);
   //std::cout << "InsertIntoLeaf::After FindLeafPage()" << endl;
  auto*  leaf = reinterpret_cast<LeafPage *> (page->GetData());
  ValueType tmp;
  //2.Find the key exists in the Leaf Node or not
  if (page!=nullptr&&leaf->Lookup(key,tmp,comparator_)==true) {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    std::cerr << "BPLUSTREE_TYPE::InsertIntoLeaf-----It already Has key" << endl;
    return state;
  } else {
    
    if (leaf != nullptr) {
    
      int size = leaf->Insert(key,value,this->comparator_);
      /*for (int i = 0; i < leaf->GetSize(); i++) cout << leaf->array_[i].first << " ";
      cout << endl;*/
      if (size >= this->leaf_max_size_) {
         //If the Leaf Node is OverFlow
         //Split LeafNode
     
        auto *NewNode = this->Split(leaf);
       
        //auto *Leaf = reinterpret_cast<LeafPage *>(NewNode);
        //this->ToString(leaf, buffer_pool_manager_);
        //this->ToString(Leaf,buffer_pool_manager_);
        KeyType NewKey = NewNode->KeyAt(0);
        
        //Insert NewKey into the ParentNode
        //std::cout << "InsertIntoLeaf::InsertIntoParent()" << endl;
        this->InsertIntoParent(leaf,NewKey,NewNode,transaction);
        
        //std::cout << "InsertIntoLeaf::AfterInsertIntoParent()" << endl;
        state = true;
      } else {
          //If the Leaf Node is  not OverFlow
       
        //this->ToString(leaf, buffer_pool_manager_);
          state = true;
          
      }
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
     
      
    } else {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      std::cerr << "out of memory" << endl;
      state = false;
    }
  }
  return state;
}


//不管什么page类型都会Split成InternalPageSize---如何处理？
/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
     page_id_t NewId = INVALID_PAGE_ID;
     auto *page = buffer_pool_manager_->NewPage(NewId);
     if (page != nullptr) {
       N *NewNode = reinterpret_cast<N *>(page->GetData());
       //Init 
       NewNode->Init(NewId, INVALID_PAGE_ID,node->GetMaxSize());
       
      
       node->MoveHalfTo(NewNode,buffer_pool_manager_);
       
       //Set the Sibbling point to the Same Parent
       NewNode->SetParentPageId(node->GetParentPageId());
       buffer_pool_manager_->UnpinPage(NewId, true);
      
       return NewNode;
     } else {
       buffer_pool_manager_->UnpinPage(NewId, false);
       std::cerr << "BPLUSTREE_TYPE::InsertIntoParent---Can not Allocate Memory AnyMore" << endl;
       return nullptr;
     }
  
}


/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
    Transaction *transaction) {
  KeyType NewKey = key;
  while (1) {
      //old_node is root
    if (old_node->GetPageId() == this->root_page_id_) {
      page_id_t NewId = INVALID_PAGE_ID;
      auto *page = buffer_pool_manager_->NewPage(NewId);
      if (page != nullptr) {
        auto *node = reinterpret_cast<InternalPage *>(page->GetData());
        node->Init(NewId, INVALID_PAGE_ID,this->internal_max_size_);
        //Generate the New Root
        node->PopulateNewRoot(old_node->GetPageId(), NewKey, new_node->GetPageId());
        //Reset Parent
        node->ResetParent(old_node->GetPageId(), new_node->GetPageId(),buffer_pool_manager_);
       
        //Update the New Root
        this->root_page_id_ = NewId;
        this->UpdateRootPageId(false);
        buffer_pool_manager_->UnpinPage(NewId, true);
        //std::cout << "BPLUSTREE_TYPE::InsertIntoParent------286" << endl;
        break;
      } 
      else {
        buffer_pool_manager_->UnpinPage(NewId, false);
        std::cerr << "BPLUSTREE_TYPE::InsertIntoParent---Can not Allocate Memory AnyMore" << endl;
      }
    } 
    else 
    {
        //Percolate Up
        //Get Parent Page From the BufferPoolManager
        page_id_t Id = old_node->GetParentPageId();
        auto *page = buffer_pool_manager_->FetchPage(Id);
        if (page != nullptr) 
        {
            auto * ParentNode = reinterpret_cast<InternalPage *>(page->GetData());
            //Insert NewKey into the Parent Node
            /*for (int i = 0; i < ParentNode->GetSize(); i++) cout << ParentNode->array_[i].first << " ";
            cout << endl;*/
            int size = ParentNode->InsertNodeAfter(old_node->GetPageId(), NewKey, new_node->GetPageId());
            /*for (int i = 0; i < ParentNode->GetSize(); i++) cout << ParentNode->array_[i].first << " ";
            cout << endl;*/
            // if the Parent Node is not OverFlow
            if (size < this->internal_max_size_) {
            buffer_pool_manager_->UnpinPage(Id, true);
              //std::cout << "BPLUSTREE_TYPE::InsertIntoParent------311" << endl;
            break;
            }
            else {
            //If the Parent Node is Full        
            KeyType middle_key = ParentNode->KeyAt(ParentNode->GetSize()-ParentNode->GetMinSize());
            InternalPage *Sibbling = this->Split(ParentNode);
            old_node = ParentNode;
            NewKey = middle_key;
            new_node = Sibbling;
            buffer_pool_manager_->UnpinPage(Id, true);
            //std::cout << "BPLUSTREE_TYPE::InsertIntoParent------321" << endl;
            continue;
            }
        } 
        else 
        {
          buffer_pool_manager_->UnpinPage(Id, false);
          //std::cerr << "BPLUSTREE_TYPE::InsertIntoParent---2Can not Allocate Memory AnyMore" << endl;
        }
    }
  }
  
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) { 
    if (this->IsEmpty()) return;
    auto page = FindLeafPage(key, false);
    auto node = reinterpret_cast<BPlusTreePage*> (page->GetData());
    page_id_t FirstLeafId = page->GetPageId();

    
  
    if (node->IsRootPage()) {
      LeafPage* Root = reinterpret_cast<LeafPage *> (node);
      int size=Root->RemoveAndDeleteRecord(key, comparator_);
      if (size == 0) {
        if (this->AdjustRoot(node)) {
          buffer_pool_manager_->UnpinPage(node->GetPageId(),true);       
          bool state=buffer_pool_manager_->DeletePage(node->GetPageId());
          if (state == false) {
            throw "fuck";
          }
        }
      }
    } 

    else {
      auto Leaf = reinterpret_cast<LeafPage *>(page->GetData());
      int size = Leaf->RemoveAndDeleteRecord(key, comparator_);
      //After the Deletetion the Leaf size >= MinSize
      if (size >= (Leaf->GetMinSize())) {
        buffer_pool_manager_->UnpinPage(FirstLeafId, true);
       
        return;
      }
    
      else {
        //After the Deletion the Leaf size < MinSize
          //Merge or Borrow
          ///
     
        if (this->CoalesceOrRedistribute(&Leaf, transaction) == false) {
          buffer_pool_manager_->UnpinPage(FirstLeafId, true);
          
          return;
        }
        else {
            // We need to Adjust Percolate Up
          
            auto ParentPage = buffer_pool_manager_->FetchPage(Leaf->GetParentPageId());
            page_id_t ParentId = ParentPage->GetPageId();
            InternalPage *node = reinterpret_cast<InternalPage*> (ParentPage->GetData());
            buffer_pool_manager_->UnpinPage(Leaf->GetPageId(), true);
            bool state=buffer_pool_manager_->DeletePage(Leaf->GetPageId());
            if (state == false) {
              throw "fuck";
            }
            InternalPage *Parent = nullptr;

          

           
            while (1) {
              if (this->CoalesceOrRedistribute(&node, transaction) == false) {
                buffer_pool_manager_->UnpinPage(FirstLeafId, true);
                buffer_pool_manager_->UnpinPage(ParentId,true);
               
                break;
              }
              else {   
                 
                Parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
                buffer_pool_manager_->UnpinPage(ParentId, true);
                buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
                bool state = buffer_pool_manager_->DeletePage(node->GetPageId());
                ParentId = Parent->GetPageId();
                if (state == false) {              
                  throw "fuck";
                }
                node = Parent;
              }
            }
        }
      
      }
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    


}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N **node, Transaction *transaction) 
{

  bool state = false;
  if (this->IsEmpty() == true) return false;
  InternalPage *ParentNode = nullptr;
  // Root Page
  if ((* node)->IsRootPage()) {
    if (( * node)->GetSize() == 1) {
      state=this->AdjustRoot(*node);
      if (state) {
     
        page_id_t PageId = (*node)->GetPageId();
        buffer_pool_manager_->UnpinPage(PageId, true);

        bool state=buffer_pool_manager_->DeletePage(PageId);
        if (state == false) {
          throw "fuck";
        }
      }
    }
    return false;
  }

      //Get the Parent Page
    page_id_t ParentPageId = (*node)->GetParentPageId();
    auto Page=buffer_pool_manager_->FetchPage(ParentPageId);
    ParentNode = reinterpret_cast<InternalPage *>(Page->GetData());
    //Get the child node index in the ParentNode 
    int index = ParentNode->ValueIndex((*node)->GetPageId());

    //If the Node is LeafNode
    if (( * node)->IsLeafPage()) {
       // If the Node is the right Most Element
      if (index == ParentNode->GetSize() - 1) {
        // node is last child of the Parent Node
        int SibblingIndex = index - 1;
        page_id_t Sibbling_Page_Id = ParentNode->ValueAt(SibblingIndex);
        // Merge or Borrow from the Left Page
        auto SibblingPage = buffer_pool_manager_->FetchPage(Sibbling_Page_Id);
        LeafPage *SibblingNode = reinterpret_cast<LeafPage *>(SibblingPage->GetData());
        LeafPage *Node = reinterpret_cast<LeafPage *>(*node); 
        //case 1----Borrow From the Sibbiling Node ----Test---Not Check
        if (SibblingNode->GetSize() + Node->GetSize() > Node->GetMaxSize()) {
          // Move the Sibbling Last Pair into the This Node
          this->Redistribute(SibblingNode, Node, 1);
          int index = ParentNode->ValueIndex(Node->GetPageId());
          ParentNode->SetKeyAt(index, Node->KeyAt(0));
          state = false;
        } 
        //case 2---- Merge with the Sibbling Node  ----Test---OK
        else {
          state = this->Coalesce(&SibblingNode, &Node, &ParentNode, index, transaction,true);
        }
        buffer_pool_manager_->UnpinPage(Sibbling_Page_Id, true);
      } 
      // if the node is not right most Node
      else {
        page_id_t Sibbling_Page_Id = ParentNode->ValueAt(index + 1);
        auto SibblingPage = buffer_pool_manager_->FetchPage(Sibbling_Page_Id);
        LeafPage *SibblingNode = reinterpret_cast<LeafPage *>(SibblingPage->GetData());
        LeafPage *Node = reinterpret_cast<LeafPage *>(*node); 
        //case 1--- Borrow From the Sibbling Node------ Test----Not Check
        if (SibblingNode->GetSize() + Node->GetSize() > Node->GetMaxSize()) {
          // Move the Redistribute First Pair into the End of this node
          this->Redistribute(SibblingNode, Node, 0);
          // Adjust the Key in the Parent Node
          int index = ParentNode->ValueIndex(SibblingNode->GetPageId());
          ParentNode->SetKeyAt(index, SibblingNode->KeyAt(0));
          state = false;
        }
        // case 2----Merge With the Sibbling Node -----Test---Ok
        else {
          state = this->Coalesce(&Node, &SibblingNode, &ParentNode, index + 1, transaction,false);
          *node = (reinterpret_cast<N*> (SibblingNode));
        }
        buffer_pool_manager_->UnpinPage(Sibbling_Page_Id, true);
      }

      buffer_pool_manager_->UnpinPage(ParentPageId, true);
    
    
   } else {
   // If the node is Internal Node
        // node is >= MinSize
     if (( * node)->GetSize() >= (*node)->GetMinSize()) {
     buffer_pool_manager_->UnpinPage(ParentPageId,true);
      
             return false;
     }
     // If the Node is the right Most Element
     if (index == ParentNode->GetSize() - 1) {
       // node is last child of the Parent Node
       int SibblingIndex = index - 1;
       page_id_t Sibbling_Page_Id = ParentNode->ValueAt(SibblingIndex);
       // Merge or Borrow from the Left Page
       auto SibblingPage = buffer_pool_manager_->FetchPage(Sibbling_Page_Id);

       InternalPage *SibblingNode = reinterpret_cast<InternalPage *>(SibblingPage->GetData());
       InternalPage *Node = reinterpret_cast<InternalPage *>(*node);

       
       // case 1----Borrow From the Sibbiling Node ----  Test Not Check
       if (SibblingNode->GetSize() + Node->GetSize() > Node->GetMaxSize()) {
         // Move the Sibbling Last Pair into the This Node
         this->Redistribute(SibblingNode, Node, 1);
         int index = ParentNode->ValueIndex(Node->GetPageId());
         ParentNode->SetKeyAt(index, Node->KeyAt(SibblingNode->GetSize()));
         state = false;
       }
       // case 2---- Merge with the Sibbling Node  ----Test Not Check
       else {
         state = this->Coalesce(&SibblingNode, &Node, &ParentNode, index, transaction,true);
       }
       buffer_pool_manager_->UnpinPage(Sibbling_Page_Id, true);
     }
     // if the node is not right most Node
     else 
     {
       page_id_t Sibbling_Page_Id = ParentNode->ValueAt(index + 1);
       auto SibblingPage = buffer_pool_manager_->FetchPage(Sibbling_Page_Id);
       
       InternalPage *SibblingNode = reinterpret_cast<InternalPage *>(SibblingPage->GetData());
       InternalPage *Node = reinterpret_cast<InternalPage *>(*node);

       // case 1--- Borrow From the Sibbling Node------ Test-Not Check
       if (SibblingNode->GetSize() + Node->GetSize() > Node->GetMaxSize()) {
         // Move the Redistribute First Pair into the End of this node
         this->Redistribute(SibblingNode, Node, 0);
         // Adjust the Key in the Parent Node
         int index = ParentNode->ValueIndex(SibblingNode->GetPageId());
         ParentNode->SetKeyAt(index, SibblingNode->KeyAt(0));
         state = false;
       }
       // case 2----Merge With the Sibbling Node -----Test-Not Check
       else {
         state = this->Coalesce(&Node, &SibblingNode, &ParentNode, index + 1, transaction,false);
         *node = (reinterpret_cast<N *>(SibblingNode));
       }
      //
       buffer_pool_manager_->UnpinPage(Sibbling_Page_Id, true);
     }

     buffer_pool_manager_->UnpinPage(ParentPageId, true);
   
   
   
   
   }
  

  
  return state;
  
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction,bool IsChange) {
  bool state = false;
    //node is LeafNode
  if (( *node)->IsLeafPage() == true) {
    LeafPage *Node = reinterpret_cast<LeafPage *>(*node);
    LeafPage *Neigbor__Node = reinterpret_cast<LeafPage *>(*neighbor_node);
    
    Node->MoveAllTo(Neigbor__Node);
    //remove the common key point to the node and neightbor_node
    (*parent)->Remove(index);
     state = true;
     
    /* if (IsChange==false) {
       std::swap(neighbor_node, node);
     }*/
  }
  // node is Internal Node
  else {
      // Get the middle_key from the Parent 
     InternalPage *Node = reinterpret_cast<InternalPage *>(*node);
     InternalPage *Neigbor__Node = reinterpret_cast<InternalPage *>(*neighbor_node);
    
     KeyType middle_key = (*parent)->KeyAt(index);
    // Merge the Node middle_key With the Neighbor_node
    (Node)->MoveAllTo(Neigbor__Node, middle_key, buffer_pool_manager_);
    (*parent)->Remove(index);
    state = true;
  
   /* if (!IsChange) {
      std::swap(neighbor_node, node);
    }*/
  }

  
  return state;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  

    //KeyType Middle_key = node->KeyAt(index);
    if (node->IsLeafPage()) {
      LeafPage *Node = reinterpret_cast<LeafPage *>(node);
      LeafPage *Neighbor_Node = reinterpret_cast<LeafPage *>(neighbor_node);
      if (index == 0) {
        Neighbor_Node->MoveFirstToEndOf(Node);
      } else {
        Neighbor_Node->MoveLastToFrontOf(Node);
      }
    } else {
      InternalPage *Node = reinterpret_cast<InternalPage *>(node);
      InternalPage *Neighbor_Node = reinterpret_cast<InternalPage *>(neighbor_node);

      auto page = buffer_pool_manager_->FetchPage(Node->GetParentPageId());
      InternalPage* Parent = reinterpret_cast<InternalPage *>(page->GetData());
      
      if (index == 0) {
        int index = Parent->ValueIndex(Neighbor_Node->GetPageId());
        KeyType middle_key = Parent->KeyAt(index);
        Neighbor_Node->MoveFirstToEndOf(Node,middle_key,buffer_pool_manager_);
      } else {
        int index = Parent->ValueIndex(Node->GetPageId());
        KeyType middle_key = Parent->KeyAt(index);
        Neighbor_Node->MoveLastToFrontOf(Node,middle_key , buffer_pool_manager_);
      }
      buffer_pool_manager_->UnpinPage(Node->GetParentPageId(), false);

    } 
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  bool state = false;
  InternalPage *Root = reinterpret_cast<InternalPage *> (old_root_node);
  if (Root->GetSize() == 0) {
    this->root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(false);
    state = true;
  } else {
    page_id_t ChildPageId=Root->RemoveAndReturnOnlyChild();
    auto page = buffer_pool_manager_->FetchPage(ChildPageId);
    auto node = reinterpret_cast<BPlusTreePage *>(page);
    node->SetParentPageId(INVALID_PAGE_ID);
    this->root_page_id_ = ChildPageId;
    UpdateRootPageId(false);
    buffer_pool_manager_->UnpinPage(ChildPageId, true);
    state = true;

  }
  return state;
   
}



/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { 
   KeyType key;    
  auto page = this->FindLeafPage(key, true);

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  page_id_t CurrPageId = leaf->GetPageId();

 
  bool state = false;

  // First Page Has Tuple
  if (leaf->GetSize() == 0)
    state = false;
  else
    state = true;

  if (state == false) {
    buffer_pool_manager_->UnpinPage(CurrPageId, false);
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, buffer_pool_manager_);
  } else {
    buffer_pool_manager_->UnpinPage(CurrPageId, false);
    int index = 0;
    return INDEXITERATOR_TYPE(CurrPageId, index, buffer_pool_manager_);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto page = this->FindLeafPage(key, false);

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  page_id_t CurrPageId = leaf->GetPageId();

  ValueType value;
  bool state=leaf->Lookup(key, value, this->comparator_);
  // if the key is not exists in the LeafPage
  if (state == false) {
    buffer_pool_manager_->UnpinPage(CurrPageId, false);
    return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, buffer_pool_manager_);
  } else {
    buffer_pool_manager_->UnpinPage(CurrPageId, false);
    int index = leaf->ValueIndex(value);
    return INDEXITERATOR_TYPE(CurrPageId, index, buffer_pool_manager_);  
  }
  
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() { return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, buffer_pool_manager_); }
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::TrackPage2(page_id_t root_page_id_, BufferPoolManager *buffer_pool_manager_) {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (node->IsLeafPage() == false) {
   InternalPage *InternalNode = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t NextPage = InternalNode->array_[0].second;
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    // Get Next Page
    page = buffer_pool_manager_->FetchPage(NextPage);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
 
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) { 
    Page *ptr = nullptr;
    if (this->IsEmpty()) return ptr;
    else {
        //Get the Root Page
      auto page = buffer_pool_manager_->FetchPage(this->root_page_id_);
      auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      //std::cout << "FindLeafPage--- The Root Page is" << node->GetPageId() << endl;
      // if the root node is leaf page
      if (node->IsLeafPage()) {
        ptr= page;
      
      } else {
      // if the root node is not leaf page
        page_id_t NextPage;
        while (node->IsLeafPage() == false) {
          InternalPage * InternalNode = reinterpret_cast<InternalPage *>(page->GetData());
          if (leftMost != true) {
            NextPage = InternalNode->Lookup(key, comparator_);
          }
          else {
              // Get the Left Most Item
            NextPage = InternalNode->ValueAt(0);
          }
          buffer_pool_manager_->UnpinPage(node->GetPageId(),false);
          //Get Next Page
          page = buffer_pool_manager_->FetchPage(NextPage);
          node = reinterpret_cast<BPlusTreePage *>(page->GetData());  
        }
        ptr= page;
      }
    }
  
    return ptr;
}


/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(bool state) { 
  auto page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID); 
  auto IndexPage = reinterpret_cast<IndexRootsPage *>(page->GetData());

  if (this->root_page_id_ == INVALID_PAGE_ID) {
    IndexPage->Delete(this->index_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    return;
  }

  if(state==true) IndexPage->Insert(this->index_id_, this->root_page_id_);
  else IndexPage->Update(this->index_id_, this->root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);   
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}


template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
