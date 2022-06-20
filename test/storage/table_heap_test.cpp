#include <vector>
#include <unordered_map>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "storage/table_heap.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields = new Fields{
            Field(TypeId::kTypeInt, i),
            Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
            Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))
    };
   
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  table_heap->FreeHeap();

}
TEST(TableHeapTest, TableHeapApplyDeleteTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  // Insert Tuple into the tableHeap
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};

    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    if(i>=row_nums/2)row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }

  // Deletion Half size of Tuple in the TableHeap
  TableIterator iter = table_heap->Begin(nullptr);
  int i;
  for (i = 0; i < row_nums/2; i++, iter++) {
    table_heap->ApplyDelete(iter->GetRowId(), nullptr);
  }
  //std::cout << row_values.size() << endl;
  ASSERT_EQ(row_nums/2, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());

    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  table_heap->FreeHeap();
 // std::cout << "success" << endl;
}

TEST(TableHeapTest, TableHeapUpdateTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  //Insert Tuple into the tableHeap
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    //row_values[row.GetRowId().Get()] = fields;
    //std::cout << row.GetRowId().Get() << endl;
    delete[] characters;
  }

  //Update Tuple in the TableHeap
  TableIterator iter = table_heap->Begin(nullptr);
  int i;
  for ( i = 0; i < row_nums; i++,iter++) {
   
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};

    Row row(*fields);
    table_heap->UpdateTuple(row, iter->GetRowId(), nullptr);
    row_values[row.GetRowId().Get()] = fields;
   // std::cout << row.GetRowId().GetPageId() << " : " << row.GetRowId().GetSlotNum() << endl;
    delete[] characters;
  }
 
  ASSERT_EQ(row_nums, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  table_heap->FreeHeap();
  //std::cout << "success" << endl;
  
}
TEST(TableHeapTest, TableHeapIteratorTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 1000;
  // create schema
  std::vector<Column *> columns = {ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
                                   ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
                                   ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    row_values[i] = fields;
    //row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  //for (auto row_kv : row_values) {
  //  Row row(RowId(row_kv.first));
  //  table_heap->GetTuple(&row, nullptr);
  //  ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
  //  for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
  //    ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
  //  }
  //  // free spaces
  //  delete row_kv.second;
  //}

  for (TableIterator iter = table_heap->Begin(nullptr); iter != table_heap->End();iter++) {
    Row row(RowId(iter->GetRowId()));
    table_heap->GetTuple(&row, nullptr);
   // std::cout << row.GetFields().size() << endl;
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
   
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {  
       //std::cout << "test---" << j << endl;
      row.GetField(j)->CompareEquals(*(iter->GetField(j)));
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(*(iter->GetField(j))));
    }
    // free spaces
  }
  table_heap->FreeHeap();
  //std::cout << "Success" << endl;
}
