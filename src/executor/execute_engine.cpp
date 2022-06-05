extern "C" {
int yyparse(void);

#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

#include "executor/execute_engine.h"
#include "glog/logging.h"
#include <fstream>
#include<iomanip>
#define ENABLE_EXECUTE_DEBUG
//
ExecuteEngine::ExecuteEngine() {
  // find the file in the bin file folder, and fill the contents in the object
  // all the private value can be initialized by using the disk manager when used.
  // add the txt file implementation, format -> every line contains a database storage file's name (of course no spaces and other white space)
  std::fstream db_contents;
  const std::string contents_name("content.txt");
  // read, do not need refreshing
  db_contents.open(contents_name, std::ios::in | std::ios::out);
  if (!db_contents.is_open()) {
    db_contents.clear();
    db_contents.open(contents_name, std::ios::trunc | std::ios::out);
    db_contents.close();
    db_contents.open(contents_name, std::ios::in | std::ios::out);
    if (!db_contents.is_open()) {
      std::cerr << "Can not open the content file!" << std::endl;
    }
  }
  // if the program keeps running to here, then the file has been opened successfully
  // the current_db_ needs to be initialized after usedatabase is executed.
  std::string db_file_names;
  while (db_contents >> db_file_names) {
    if (!db_file_names.empty()) {
      DBStorageEngine *store_eng = new DBStorageEngine(db_file_names, false);
      this->dbs_.emplace(db_file_names, store_eng);
    }
  }
  db_contents.close();
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast, ExecuteContext *context) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  switch (ast->type_) {
      //yhm
    case kNodeCreateDB://-ok
      return ExecuteCreateDatabase(ast, context);
      //yhm
    case kNodeDropDB://-ok
      return ExecuteDropDatabase(ast, context);
      //cjx
    case kNodeShowDB://-ok
      return ExecuteShowDatabases(ast, context);
      //cjx
    case kNodeUseDB://-ok
      return ExecuteUseDatabase(ast, context);
      //cjx
    case kNodeShowTables://--ok
      return ExecuteShowTables(ast, context);
      //cjx
    case kNodeCreateTable://--ok
      return ExecuteCreateTable(ast, context);
      //yhm
    case kNodeDropTable://--ok
      return ExecuteDropTable(ast, context);
      //yhm
    case kNodeShowIndexes://--ok
      return ExecuteShowIndexes(ast, context);
      //yhm
    case kNodeCreateIndex://-ok
      return ExecuteCreateIndex(ast, context);
      //yhm
    case kNodeDropIndex://--ok
      return ExecuteDropIndex(ast, context);
      //select * from table ---yhm&cjx
    case kNodeSelect://-ok
      return ExecuteSelect(ast, context);
      //cjx---ok
    case kNodeInsert://-ok
      return ExecuteInsert(ast, context);
      //yhm
    case kNodeDelete:
      return ExecuteDelete(ast, context);
      //yhm
    case kNodeUpdate:
      return ExecuteUpdate(ast, context);
      //No
    case kNodeTrxBegin://-ok
      return ExecuteTrxBegin(ast, context);
      //No
    case kNodeTrxCommit://-ok
      return ExecuteTrxCommit(ast, context);
      //No
    case kNodeTrxRollback://-ok
      return ExecuteTrxRollback(ast, context);
      //cjx
    case kNodeExecFile://-ok
      return ExecuteExecfile(ast, context);
      //Complished
    case kNodeQuit://-ok
      return ExecuteQuit(ast, context); // nop
    default:
      break;
  }
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  pSyntaxNode ast_child = ast->child_;
  std::string db_file_name(ast_child->val_);
  // 1. first search whether the name is duplicated.
  auto iter_dbs = this->dbs_.find(db_file_name);
  if (iter_dbs != this->dbs_.end()) {
    // the dbs_ is found, you can not create it
    std::cerr << "The database has existed. You can use it, but can not create a duplicated database!" << std::endl;
    printf("DATABASE CREATION ");
    return DB_FAILED;
  }
  // 2. if the database is not duplicated, then create a new database.
  DBStorageEngine *store_eng = new DBStorageEngine(db_file_name, true);  // create a new database
  this->dbs_.emplace(db_file_name, store_eng);
  printf("DATABASE CREATION ");
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  // 1. find the database in the list, if the database does not exist now, then return FAILED operation.
  std::string dest_db(ast->child_->val_);
  auto iter_dbs = this->dbs_.find(dest_db);
  if (iter_dbs == this->dbs_.end()) {
    // if the desired database is not found, then return failed.
    std::cout << "This database is not created before, thus it can not be dropped!" << std::endl;
    printf("DROP DATABASE ");
    return DB_FAILED;
  }
  // 2. the database is in the list, can be deleted.
  delete iter_dbs->second; // clear the DatabaseStorage engine out of memory
  remove(dest_db.c_str()); // delete the file
  if (this->current_db_ == dest_db) {
    // the current db need to be set to empty
    this->current_db_.clear();
  }
  this->dbs_.erase(iter_dbs); // erase the elements provided by the key
  printf("DROP DATABASE ");
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
 
    std::unordered_map<std::string, DBStorageEngine *>::iterator iter;
    std::cout << "+-----------------------------------+" << endl;
    std::cout << "| Databases                         |" << endl;
    std::cout << "+-----------------------------------+" << endl;
    for (iter = this->dbs_.begin(); iter != this->dbs_.end(); iter++) {
      string db_name = iter->first;
      std::cout << "| " << left << setw(34) << db_name <<'|' << endl;
    }
    std::cout << "+-----------------------------------+" << endl;
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  pSyntaxNode ast_child_ = ast->child_;
  std::string db_name(ast_child_->val_);
  auto iter_dbs = this->dbs_.find(db_name);

  if (iter_dbs == this->dbs_.end()) {
    std::cerr << "The database " << db_name << " is not exist." << endl;
    return DB_FAILED;
  } else {
    this->current_db_ = db_name;
    std::cout << "Default schema set to " << db_name << endl;
    return DB_SUCCESS;
  }
  
}
//Show Tables
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  std::unordered_map<std::string, DBStorageEngine *>::iterator iter = this->dbs_.find(this->current_db_);
  //Get the Current Storage from the Disk
  std::vector<std::string > CurrentTables;
  dberr_t state;
  if (iter != this->dbs_.end()) {
    DBStorageEngine *CurrentStorage = iter->second;
    state= CurrentStorage->catalog_mgr_->GetAllTableNames(CurrentTables);
    if (state==DB_SUCCESS)  {
      std::cout << "+-----------------------------------+" << endl;
      std::cout << "| " << left << setw(34) << this->current_db_ <<  '|' << endl;
      std::cout << "+-----------------------------------+" << endl;
      for (auto i : CurrentTables) {
        std::cout << "| " << left << setw(34) << i << '|' << endl;
      }
      std::cout << "+-----------------------------------+" << endl;
      state = DB_SUCCESS;
    } else {
      std::cout << "There are no tables exists in the " << this->current_db_ << endl;
      state = DB_FAILED;
    }
  }
  
  return state;
}
// Note:Primary Key Definition must be End of the table
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  // if the Current_db is empty
  if (true == this->current_db_.empty()) {
    return DB_FAILED;
  } else 
  {
    
    // 1.Get the Storage of the DbstorageEngine
    auto StorageEngine_Iter = this->dbs_.find(this->current_db_);
    auto Current_Storage_Engine = StorageEngine_Iter->second;
    auto Current_Ctr = Current_Storage_Engine->catalog_mgr_;

    // 2. Get the Name of the Table
    pSyntaxNode ast_child_Table_Name = ast->child_;
    std::string TableName(ast_child_Table_Name->val_);
    // 3. Get the Schema of the Table
    TableInfo *NewTableInfo = nullptr;
    std::vector<Column *> Columns;
    
    // step1:tmp start at the Column Definition
    int index = 0;
    bool PkCheck = false;
    for (pSyntaxNode tmp = ast->child_->next_->child_; tmp != nullptr;) 
    {
      // Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique);
      int order = 0;
      bool unique = false;//ok
      std::string Column_name;//ok
      TypeId type;//ok
      uint32_t length = 0;//ok
      int table_ind = index;//ok
      bool nullable = true;

      for (pSyntaxNode ColumnDefinitionNode = tmp; ColumnDefinitionNode != NULL;) 
      {
        if ((ColumnDefinitionNode->val_==nullptr) ||
            (ColumnDefinitionNode->val_ && std::string(ColumnDefinitionNode->val_) != std::string("primary keys"))) 
            {
                // Get the Attribute of the Uniqueness
                if (order == 0) 
                {
                  if (ColumnDefinitionNode->val_) 
                  {
                    std::string Attribute(ColumnDefinitionNode->val_);
                    if (Attribute == std::string("unique")) {
                      unique = true;
                    }
                  }
                    ColumnDefinitionNode = ColumnDefinitionNode->child_;
                    order++;
                    continue;
                }
                // Get the ColumnName of the Table
                else if (order == 1) 
                {
                        Column_name = ColumnDefinitionNode->val_;
                        ColumnDefinitionNode = ColumnDefinitionNode->next_;
                        order++;
                        continue;
                }
                // Get the Column Type or Length of the Char
                else if (order == 2) 
                {
                
                    std::string TypeVariable = (ColumnDefinitionNode->val_);
                    // Type is int
                    if (TypeVariable == std::string("int")) 
                    {
                        type = kTypeInt;
                        length = sizeof(int32_t);
                    }
                    // Type is  Float
                    else if (TypeVariable == std::string("float")) 
                    {
                        type = kTypeFloat;
                      length = sizeof(float_t);
                    }

                    // Type is Char
                    else     
                    {
                     type = kTypeChar;
                    // Get the Length of the char
                    ColumnDefinitionNode = ColumnDefinitionNode->child_;
                    std::string CharLength = (ColumnDefinitionNode->val_);
                    int32_t intStr = atoi(CharLength.c_str());
                    length = intStr;
                    }
                    order++;
                    ColumnDefinitionNode = ColumnDefinitionNode->next_;
                    continue;
                } 
                
               
            } 
            else 
            {
             
                //Set the Primary Key-- NotNullAble+ unique
              for (pSyntaxNode tmp = ColumnDefinitionNode->child_; tmp != nullptr; tmp = tmp->next_) 
              {
                  std::vector<Column*>::iterator iter;
                  for (iter = Columns.begin(); iter != Columns.end(); iter++) 
                  {
                        if ((*iter)->GetName() == std::string(tmp->val_)) 
                        {
                            //Primary Key can not be null , should be unique
                            (*iter)->SetNullable(false);
                            (*iter)->SetUnique(true);
                            break;
                        }
                  }
                   
              }
              PkCheck = true;
              break;
            }
      }
      // Push Column into the Vector
      //  Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
      if (!PkCheck) {
        if ((type == kTypeInt || type == kTypeFloat)) {
          void *buf = Current_Ctr->GetMemHeap()->Allocate(sizeof(Column));
          Column *tmp = new (buf) Column(Column_name, type, table_ind, nullable, unique);
          Columns.push_back(tmp);
        }
        // Type is char
        // Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool
        // unique);
        else {
          void *buf = Current_Ctr->GetMemHeap()->Allocate(sizeof(Column));
          Column *tmp = new (buf) Column(Column_name, type, length, table_ind, nullable, unique);
          Columns.push_back(tmp);
        }

        tmp = tmp->next_;
        index++;
      } else {
        break;
      }
      
    }
 
  
    Schema tmp(Columns);
 
    //After We get the Schema From the Syntax Tree-Create Table
    dberr_t state = Current_Ctr->CreateTable(TableName, tmp.DeepCopySchema(&tmp,Current_Ctr->GetMemHeap()), nullptr, NewTableInfo);    
    if (state == DB_TABLE_ALREADY_EXIST || state == DB_FAILED) 
    {
      return DB_FAILED;
    }
    // maybe should use the NewTableInfo to do something when succeed.
  }
  return DB_SUCCESS;
}

   


dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (this->current_db_.empty()) {
    std::cerr << "Select a database first!" << std::endl;
    return DB_FAILED;
  }

  auto iter_db_file = this->dbs_.find(this->current_db_);
  if (iter_db_file == this->dbs_.end()) {
    std::cerr << "The database does not exist!" << std::endl; // this should be prevented by use executor
    return DB_FAILED;
  }

  // now the file has been found
  DBStorageEngine *storage = iter_db_file->second;
  CatalogManager *catalog = storage->catalog_mgr_;
  pSyntaxNode child = ast->child_;
  std::string deleted_tbl(child->val_);
  return catalog->DropTable(deleted_tbl);
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (this->current_db_.empty()) {
    std::cerr << "Select a database first!" << std::endl;
    return DB_FAILED;
  }

  auto iter_db_file = this->dbs_.find(this->current_db_);
  if (iter_db_file == this->dbs_.end()) {
    std::cerr << "The database does not exist!" << std::endl;  // this should be prevented by use executor
    return DB_FAILED;
  }

  // now the file has been found
  DBStorageEngine *storage = iter_db_file->second;
  CatalogManager *catalog = storage->catalog_mgr_;
  std::vector<std::string> table_names;
  if (catalog->GetAllTableNames(table_names) != DB_SUCCESS) {
    return DB_FAILED;
  }

  for (auto i:table_names) {
    std::vector<std::string> index_names;
    catalog->GetAllIndexNames(i, index_names);
    std::cout << "+-------------------------------------+" << endl;
    std::cout << "| " << left << setw(36) << i << '|' << endl;
    std::cout << "+-------------------------------------+" << endl;
    for (auto j : index_names) {
      std::cout << "| " << left << setw(36) << j << '|' << endl;
    }
    std::cout << "+-------------------------------------+" << endl << std::endl;
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (this->current_db_.empty()) {
    // if the current_db_ is empty
    std::cerr << "You have not specify the Database you are going to use!" << std::endl;
    return DB_FAILED;
  }
  auto iter_dbs = this->dbs_.find(this->current_db_);
  if (iter_dbs == this->dbs_.end()) {
    return DB_FAILED; // should be prevented in the use execution, so this can not happen
  }
  DBStorageEngine *cur_db = iter_dbs->second;
  CatalogManager *cata = cur_db->catalog_mgr_;

  // Get the nodes out
  pSyntaxNode child = ast->child_;
  std::string idx_name(child->val_);
  std::string tbl_name(child->next_->val_);
  pSyntaxNode turns = child->next_->next_;
  std::string turn_flag(turns->val_);
  std::string tmp = "index keys";
  pSyntaxNode keys_node = turns->child_;
  if (turn_flag != tmp) {
    return DB_FAILED;
  }
  std::vector<std::string> idx_keys;
  while (keys_node) {
    // no need to deal with the case with no keys -> the parser will do the check
    std::string tmp(keys_node->val_);
    idx_keys.push_back(tmp);
    keys_node = keys_node->next_;
  }
  // the information has been traited out from the parser tree.
  IndexInfo *IFO = nullptr;
  dberr_t rst = cata->CreateIndex(tbl_name, idx_name, idx_keys, nullptr, IFO); 
  if (rst == DB_SUCCESS) {
    // should do something about IFO
  }

  return rst;
}

dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  // as a consequence of a problem in the design of parser tree, so here we can not deal with the case 
  // with multi-indexes have the same name.
  // here the solution is: we delete all the indexes which has the same name as the instruction.

  // Get the information in the instruction out.
  pSyntaxNode child = ast->child_;
  std::string del_idx_name(child->val_);
  if (this->current_db_.empty()) {
    std::cerr << "Select a database first!" << std::endl;
    return DB_FAILED;
  }

  // do the general file selection
  auto iter_db_file = this->dbs_.find(this->current_db_);
  if (iter_db_file == this->dbs_.end()) {
    std::cerr << "The database does not exist!" << std::endl;  // this should be prevented by use executor
    return DB_FAILED;
  }

  // now the file has been found
  DBStorageEngine *storage = iter_db_file->second;
  CatalogManager *catalog = storage->catalog_mgr_;
  std::vector<std::string> table_names;
  if (catalog->GetAllTableNames(table_names) != DB_SUCCESS) {
    return DB_FAILED;
  }
  for (auto i : table_names) {
    IndexInfo *IFO = nullptr;
    catalog->GetIndex(i, del_idx_name, IFO);
    if (IFO == nullptr) {
      continue;
    }
    std::string cur_idx_name = IFO->GetIndexName();
    if (catalog->DropIndex(i, cur_idx_name, true) != DB_SUCCESS) {
      return DB_FAILED;
    }
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteSelect(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteSelect" << std::endl;
#endif
  // Step0: Prepare StorageEngine and CatalogManager
  // Get the Storage of the DbstorageEngine
  auto StorageEngine_Iter = this->dbs_.find(this->current_db_);
  auto Current_Storage_Engine = StorageEngine_Iter->second;
  auto Current_Ctr = Current_Storage_Engine->catalog_mgr_;
  // Get the TableName
  pSyntaxNode ast_TableName = ast->child_->next_;
  std::string TableName(ast_TableName->val_);
  TableInfo *CurTableInfo = nullptr;
  dberr_t state = Current_Ctr->GetTable(TableName, CurTableInfo);
  //Traverse the TableHeap
  if (state != DB_TABLE_NOT_EXIST) {
    TableHeap *CurTableHeap = CurTableInfo->GetTableHeap();
    
    std::vector<Field *> Fields;
    //Print the Table Name
    std::cout << "+-------------------------------------+" << endl;
    std::cout << "| " << left << setw(36) << TableName << '|' << endl;
    std::cout << "+-------------------------------------+" << endl;
    
    Schema *CurSchema = CurTableInfo->GetSchema();
    std::vector<Column *> CurColumns = CurSchema->GetColumns();

    for (auto i : CurColumns) {
      std::cout << left << setw(12)<< i->GetName() << "\t";
    }
    std::cout << endl;
    for (TableIterator iter = CurTableHeap->Begin(nullptr); iter != CurTableHeap->End(); iter++) {
    
      Fields = iter->GetFields();
      int length = iter->GetFieldCount();
     
      for (int i=0;i<length;i++) { 
        
        std::cout <<left << setw(12) << Fields[i]->GetData() << "\t"; 
      }
      state = DB_SUCCESS;
      std::cout << endl;

    }
    std::cout << "+-------------------------------------+" << endl;
  }
  return state;
}
//NOTE:: Due to Index Part Has not be Implemented , So Insert to Index not Implemented yet
dberr_t ExecuteEngine::ExecuteInsert(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteInsert" << std::endl;
#endif
  if (this->current_db_.empty()) 
  {
    std::cerr << "Choose the DataBase First" << std::endl;
    return DB_FAILED;
  } 
  else 
  {
    // Step0: Prepare StorageEngine and CatalogManager
    // Get the Storage of the DbstorageEngine
    auto StorageEngine_Iter = this->dbs_.find(this->current_db_);
    auto Current_Storage_Engine = StorageEngine_Iter->second;
    auto Current_Ctr = Current_Storage_Engine->catalog_mgr_;

    // Step1: Check the Table is in the CatalogManager or not
    // Get the Table Name
    pSyntaxNode ast_TableName = ast->child_;
    std::string TableName = (ast_TableName->val_);
    TableInfo *CurTableInfo = nullptr;
    dberr_t state =Current_Ctr->GetTable(TableName, CurTableInfo);
    std::vector<Field> Fields;
    // Get the MemHeap
    MemHeap *CurMemHeap = CurTableInfo->GetMemHeap();

    //Table is not Exists in the Current Database
    if (state == DB_TABLE_NOT_EXIST) 
    {
        std::cerr << "Choose the DataBase First" << std::endl;
        return DB_FAILED;
    } 
    else {
      // 0. Get the Shema of the Table
      Schema *CurSchema = CurTableInfo->GetSchema();
      std::vector<Column *> Columns = CurSchema->GetColumns();
      int CurPosition = 0;
      dberr_t state;

      for (pSyntaxNode ColumnNode = ast_TableName->next_->child_; ColumnNode != nullptr;
           ColumnNode = ColumnNode->next_, CurPosition++) {
        // 1. Check Value Type
        // 2. Check Not null
        // 3. Get the Entity of the Fields
        switch (ColumnNode->type_) {
          case SyntaxNodeType::kNodeNull:
            // Current Column can not be null
            if (Columns[CurPosition]->IsNullable() == false) {
              state = DB_FAILED;
            } else {
              char *mem = (char *)CurMemHeap->Allocate(sizeof(char) * 5);
              strcpy(mem, "null");
              Fields.push_back(Field(kTypeChar, mem, 5, true));
              state = DB_SUCCESS;
            }
            break;

          case SyntaxNodeType::kNodeNumber:
            if (Columns[CurPosition]->GetType() == kTypeInt || Columns[CurPosition]->GetType() == kTypeFloat) {
              if (Columns[CurPosition]->GetType() == kTypeInt) {
                std::string str(ColumnNode->val_);
                int Number = atoi(str.c_str());
                Fields.push_back(Field(kTypeInt, Number));
              } else if (Columns[CurPosition]->GetType() == kTypeFloat) {
                std::string str(ColumnNode->val_);
                float f = atof(str.c_str());
                Fields.push_back(Field(kTypeFloat, f));
              }
              state = DB_SUCCESS;

            } else {
              state = DB_FAILED;
            }
            break;

          case SyntaxNodeType::kNodeString:
            if (Columns[CurPosition]->GetType() == kTypeChar) {
              Fields.push_back(Field(kTypeChar, ColumnNode->val_, Columns[CurPosition]->GetLength(), true));
              state = DB_SUCCESS;
            } else {
              state = DB_FAILED;
            }
            break;

          default:
            state = DB_FAILED;
            break;
        }
        if (state == DB_FAILED) {
          return state;
        }

        // 3. Check Unique
        // Traverse the TableHeap to Check the New Inserted Column is Unique or Not
        if (Columns[CurPosition]->IsUnique() == true) {
          TableHeap *CurTableHeap = CurTableInfo->GetTableHeap();
          for (TableIterator iter = CurTableHeap->Begin(nullptr); iter != CurTableHeap->End(); iter++) {
            // if there is value in the Table Heap is Equal with the NewInserted Tuple
            if (iter->GetField(CurPosition)->CompareEquals(Fields[CurPosition]) == kTrue) {
              state = DB_FAILED;
              break;
            }
          }
        }
        if (state == DB_FAILED) {
          return state;
        }
      }
      // 4. Insert Tuple
      TableHeap *CurTableHeap = CurTableInfo->GetTableHeap();
      Row NewRow(Fields);
      bool InsertState = CurTableHeap->InsertTuple(NewRow, nullptr);
      if (InsertState) {
        return DB_SUCCESS;
      } else {
        std::cout << "InsertTuple Failed" << endl;
        return DB_FAILED;
      }
      // 5. Check the Index- If Exists Index, we need to Update the Index too.
    }
  }
  
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteDelete(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDelete" << std::endl;
#endif
  // working on it;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteUpdate(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUpdate" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string FileName(ast->child_->val_);
  std::ifstream fin;
  fin.open(FileName, std::ios::in);
  if (!fin) {
    cout << "Open Failed" << endl;
    return DB_FAILED;
  }
  char line[1024] = {0};
  // int buf_size = 1024;
  string tmp;

  while (fin.getline(line, sizeof(line))) {
    std::stringstream word(line);
    string result;
    int flag = 0;
    while (word) {
      word >> tmp;
      if (flag != 0) result += " ";
      result += tmp;
      flag++;
    }
    cout << result << endl;
    YY_BUFFER_STATE bp = yy_scan_string(line);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      printf("[INFO] Sql syntax parse ok!\n");
    }

    ExecuteContext context;
    dberr_t exe_rst = Execute(MinisqlGetParserRootNode(), &context);
    if (exe_rst == DB_SUCCESS) {
      printf("EXECUTE SUCCESS\n");
    } else if (exe_rst == DB_FAILED) {
      printf("EXECUTE FAILED\n");
    }

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    if (context.flag_quit_) {
      printf("bye!\n");
      break;
    }
  }

  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  context->flag_quit_ = true;
  return DB_SUCCESS;
}


