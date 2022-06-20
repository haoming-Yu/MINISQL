#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  // Linear Search the tableHeap, Find the Empty Page
  for (page_id_t i = this->GetFirstPageId(); i != INVALID_PAGE_ID;) {
    auto Page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
    // if the Tuple is Larger than PageSize
    if (row.GetSerializedSize(schema_) > Page->SIZE_MAX_ROW) return false;

    // If Find one Insert Tuple,and Update RowId
    Page->WLatch();
    bool state = Page->InsertTuple(row, this->schema_, txn, this->lock_manager_, this->log_manager_);
    Page->WUnlatch();
    if (state == true) {
      buffer_pool_manager_->UnpinPage(i, true);

      return true;
    } else {
      // it means current page can not allocate thi s page
      buffer_pool_manager_->UnpinPage(i, false);
      // Situation1:if the Current Page's Next Page Id is valid, it means do not need to set the Next Page id
      if (Page->GetNextPageId() != INVALID_PAGE_ID) {
        i = Page->GetNextPageId();
        continue;
      } else {
        // Situation2:if the Current Page's Next Page Id is Invalid, it means need to set the Next Page id
        // Allocate New Page
        i = AllocateNewPage(i, buffer_pool_manager_, txn, lock_manager_, log_manager_);
        Page->SetNextPageId(i);
        buffer_pool_manager_->UnpinPage(i, true);
      }
    }

    i = Page->GetNextPageId();
  }
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  return true;
}

/**
 * if the new tuple is too large to fit in the old page, return false (will delete and insert)
 * @param[in] row Tuple of new row
 * @param[in] rid Rid of the old tuple
 * @param[in] txn Transaction performing the update
 * @return true is update is successful.
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // Get OldRow
  Row OldRow(rid);
  // using UpdateTuple to update the Tuple
  int state = page->UpdateTuple(row, &OldRow, schema_, txn, lock_manager_, log_manager_);
  bool result = true;
  // Situation1: it is Invalid_Slot_number
  if (state == INVALID_SLOT_NUMBER) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    result = false;

  }
  // Situation2: it is Already Deleted.
  else if (state == TUPLE_DELETED) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    result = false;

  }
  // Situation3: it is not enough Space to Update into Current Page
  else if (state == NOT_ENOUGH_SPACE) {
    // DeleteTuple Insert into Other Page
    page->ApplyDelete(rid, txn, log_manager_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    if (this->InsertTuple(row, txn)) {
      result = true;
    } else {
      result = false;
    }

  } else if (state == 1) {
    // Replace Record on Original Place
    row.SetRowId(rid);
    // UnpinPage and update page into disk until lru replacer replace it.
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    result = true;
  }
  return result;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));

  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();

  // UnpinPage To Delete Page
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  // Free All the Page
  /*for (page_id_t i = GetFirstPageId(); i != INVALID_PAGE_ID;) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
    std::cout << i << " Page"
              << "pin_Count: " << page->GetPinCount() << endl;
    i = page->GetNextPageId();
  }*/
  /*
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(2));
  RowId i;
  RowId next;
  for (page->GetFirstTupleRid(&i); page->GetNextTupleRid(i,&next) != false;i=next) {
    cout << i.GetPageId() << " Page " << i.GetSlotNum() << " Slot" << endl;
  }
  for (page_id_t i = GetFirstPageId(); i != INVALID_PAGE_ID;) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
    std::cout << "page: " << i << "pin_count: " << page->GetPinCount() << endl;
    i = page->GetNextPageId();
  }*/
  page_id_t next = GetFirstPageId();
  for (page_id_t i = GetFirstPageId(); i != INVALID_PAGE_ID; i = next) {
    Page *page_orig = buffer_pool_manager_->FetchPage(i);
    auto page = reinterpret_cast<TablePage *>(page_orig->GetData());
    if (page->GetPinCount() > 0) buffer_pool_manager_->UnpinPage(i, false);
    next = page->GetNextPageId();
    bool state = buffer_pool_manager_->DeletePage(i);
    if (state == false) {
      std::cerr << "TableHeap::FreeHeap Failed" << endl;
    }
  }
  // Free All the Schema
  std::vector<Column *> columns = schema_->GetColumns();
  for (size_t i = 0; i < schema_->GetColumnCount(); i++) {
    columns.pop_back();
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage((row->GetRowId()).GetPageId()));
  bool state = page->GetTuple(row, this->schema_, txn, this->lock_manager_);
  buffer_pool_manager_->UnpinPage((row->GetRowId()).GetPageId(), false);
  return state;
}
page_id_t TableHeap::AllocateNewPage(page_id_t last_page_id, BufferPoolManager *buffer_pool_manager_, Transaction *txn,
                                     LockManager *lock_manager, LogManager *log_manager) {
  page_id_t new_page_id = INVALID_PAGE_ID;
  TablePage *NewPage = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  NewPage->Init(new_page_id, last_page_id, log_manager, txn);
  NewPage->SetNextPageId(INVALID_PAGE_ID);
  return new_page_id;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  TablePage *Page = nullptr;
  RowId row_id;
  // Find Valid Page
  for (page_id_t i = this->GetFirstPageId(); i != INVALID_PAGE_ID;) {
    Page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(i));
    bool state = Page->GetFirstTupleRid(&row_id);
    buffer_pool_manager_->UnpinPage(Page->GetPageId(), false);
    if (state == true) {
      break;
    }

    i = Page->GetNextPageId();
  }
  // current Rid,current page tuple count

  char *position = nullptr;
  position = Page->GetData() + Page->position_calculate(0);
  buffer_pool_manager_->UnpinPage(this->first_page_id_, false);
  return TableIterator(row_id, position, buffer_pool_manager_, this->schema_, Page);
}

TableIterator TableHeap::End() {
  RowId tmp;
  tmp.Set(INVALID_PAGE_ID, 0);
  return TableIterator(tmp, nullptr, nullptr, nullptr, nullptr);
}
