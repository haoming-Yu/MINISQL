#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeInsertTests, DISABLED_SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
  BPlusTree<int, int, BasicComparator<int>> tree1(0, engine.bpm_, comparator, 4, 4);
  BPlusTree<int, int, BasicComparator<int>> tree2(0, engine.bpm_, comparator, 4, 4);
  BPlusTree<int, int, BasicComparator<int>> tree3(0, engine.bpm_, comparator, 4, 4);
  BPlusTree<int, int, BasicComparator<int>> tree4(0, engine.bpm_, comparator, 4, 4);
  BPlusTree<int, int, BasicComparator<int>> tree5(0, engine.bpm_, comparator, 4, 4);
  BPlusTree<int, int, BasicComparator<int>> tree6(0, engine.bpm_, comparator, 4, 4);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 15;
  vector<int> keys;
  vector<int> values;
  vector<int> delete_seq;
  map<int, int> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(i);
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  
  //// Insert data
  //for (int i = 0; i < n; i++) {
  //  std::cout << i + 1 << " turns: " << keys[i] << " " << values[i] << endl;
  //  tree.Insert(keys[i], values[i]);
  //  //tree.PrintTree(mgr[0]);
  //}
  //ASSERT_TRUE(tree.Check());
  //// Print tree
  //tree.PrintTree(mgr[0]);
  // Testing Case : 11 8 5 7 4 1 14 6 3 12 10 0 2 9 13
  
  keys[0] = 11;
  keys[1] = 8;
  keys[2] = 5;
  keys[3] = 7;
  keys[4] = 4;
  keys[5] = 1;
  keys[6] = 14;
  keys[7] = 6;
  keys[8] = 3;
  keys[9] = 12;
  keys[10] = 10;
  keys[11] = 0;
  keys[12] = 2;
  keys[13] = 9;
  keys[14] = 13;
  for (int i = 0; i < n; i++) delete_seq[i] = keys[i];

  for (int i = 0; i < n; i++) values[i] = keys[i]+2;
  //std::cout << "Input Size is 4----------------------------------" << endl;
  //// Size is 4
  //for (int i = 0; i < 4; i++) {
  //  std::cout << i + 1 << " turns: " << keys[i] << " " << values[i] << endl;
  //  tree.Insert(keys[i], values[i]);
  //   tree.PrintTree(mgr[0]);
  //}
  //tree.PrintTree(mgr[0]);
  //std::cout << "Input Size is 5----------------------------------" << endl;
  
  //Size is 5
  for (int i = 0; i < n; i++) {
    mgr[0] << i + 1 << " turns: " << keys[i] << " " << values[i] <<endl;
    tree.Insert(keys[i], values[i]);
    //tree.PrintTree(mgr[0]);
  }
  tree.PrintTree(mgr[0]);
  
  tree.Destroy();
  ASSERT_TRUE(tree.Check());

  // int ans = 0;
  //for (auto iter = tree.Begin(); iter != tree.End(); ++iter, ans ++) {
  //   EXPECT_EQ(ans, (*iter).first);
  //  EXPECT_EQ(ans+2, (*iter).second);
  //}

  /*for (int i = 0; i < n; i++) {
    mgr[0] << i + 1 << " turns: " << keys[i] << endl;
    if (i == 2) {
      tree.Remove(keys[i], nullptr);
      tree.PrintTree(mgr[0]);
      break;
    } else {
      tree.Remove(keys[i], nullptr);
      tree.PrintTree(mgr[0]);
    }
  }*/
 /* std::cout << "Input Size is 6----------------------------------" << endl;
  Insert size-6
  for (int i = 0; i < 6; i++) {
    std::cout << i + 1 << " turns: " << keys[i] << " " << values[i] << endl;
    if (i ==  5) {
      tree2.Insert(keys[i], values[i]);
    } else {
      tree2.Insert(keys[i], values[i]);
    }
     if(i>=3)tree2.PrintTree(mgr[0]);
  }*/
  //tree2.PrintTree(mgr[0]);
  //std::cout << "Input Size is 7----------------------------------" << endl;
  //// Insert size-7
  //for (int i = 0; i < 7; i++) {
  //  std::cout << i + 1 << " turns: " << keys[i] << " " << values[i] << endl;
  //  tree3.Insert(keys[i], values[i]);
  //  // tree.PrintTree(mgr[0]);
  //}
  //tree3.PrintTree(mgr[0]);
  //std::cout << "Input Size is 8----------------------------------" << endl;
  // Insert size-10
 /* for (int i = 0; i < 10; i++) {
    std::cout << i + 1 << " turns: " << keys[i] << " " << values[i] << endl;
    if(i!=9)tree4.Insert(keys[i], values[i]);
    else {
      tree4.Insert(keys[i], values[i]);
    }
     tree4.PrintTree(mgr[0]);
  }
  tree4.PrintTree(mgr[0]);*/
  // Print tree
  //tree.PrintTree(mgr[0]);
  // Search keys
  /*vector<int> ans;
  for (int i = 0; i < n; i++) {
    tree.GetValue(i, ans);
    if (i == 0) mgr[0] << "original Sequeunce ";
    mgr[0] << kv_map[i] << " ";
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  ASSERT_TRUE(tree.Check());*/
  //
  //tree.PrintTree(mgr[1]);
  //// Check valid
  //ans.clear();
  //for (int i = 0; i < n / 2; i++) {
  //  ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
  //}
  //for (int i = n / 2; i < n; i++) {
  //  ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
  //  ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  //}
}

//#include "index/b_plus_tree.h"
//#include "common/instance.h"
//#include "gtest/gtest.h"
//#include "index/basic_comparator.h"
//#include "utils/tree_file_mgr.h"
//#include "utils/utils.h"
//
//static const std::string db_name = "bp_tree_insert_test.db";
//TEST(BPlusTreeTests, DISABLED_asc_Insert) {
//  // Init engine
//  DBStorageEngine engine(db_name);
//  std::cout << engine.bpm_->CheckAllUnpinned() << std::endl;
//  BasicComparator<int> comparator;
//  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//  TreeFileManagers mgr("tree_");
//  // Prepare data
//  const int n = 100;
//  vector<int> keys;
//  vector<int> values;
//  vector<int> delete_seq;
//  map<int, int> kv_map;
//  for (int i = 0; i < n; i++) {
//    keys.push_back(i);
//    values.push_back(i);
//    delete_seq.push_back(i);
//  }
//  // Shuffle data
//  // ShuffleArray(keys);
//  // ShuffleArray(values);
//  // ShuffleArray(delete_seq);
//  // Map key value
//  for (int i = 0; i < n; i++) {
//    kv_map[keys[i]] = values[i];
//  }
//  // Insert data
//  for (int i = 0; i < n; i++) {
//    tree.Insert(keys[i], values[i]);
//    //tree.PrintTree(mgr[i]);
//    ASSERT_TRUE(tree.Check());
//  }
//
//  // Print tree
// // tree.PrintTree(mgr[0]);
//  // Search keys
//  vector<int> ans;
//  for (int i = 0; i < n; i++) {
//    tree.GetValue(i, ans);
//    ASSERT_EQ(kv_map[i], ans[i]);
//  }
//  ASSERT_TRUE(tree.Check());
//}
//
//TEST(BPlusTreeTests, DISABLED_dsc_Insert) {
//  // Init engine
//  DBStorageEngine engine(db_name);
//  BasicComparator<int> comparator;
//  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//  TreeFileManagers mgr("tree_");
//  // Prepare data
//  const int n = 100;
//  vector<int> keys;
//  vector<int> values;
//  vector<int> delete_seq;
//  map<int, int> kv_map;
//  for (int i = n - 1; i >= 0; i--) {
//    keys.push_back(i);
//    values.push_back(i);
//    delete_seq.push_back(i);
//  }
//  // Shuffle data
//  // ShuffleArray(keys);
//  // ShuffleArray(values);
//  // ShuffleArray(delete_seq);
//  // Map key value
//  for (int i = 0; i < n; i++) {
//    kv_map[keys[i]] = values[i];
//  }
//  // Insert data
//  for (int i = 0; i < n; i++) {
//    tree.Insert(keys[i], values[i]);
//   // tree.PrintTree(mgr[i]);
//    ASSERT_TRUE(tree.Check());
//  }
//
//  // Print tree
//  //tree.PrintTree(mgr[0]);
//  // Search keys
//  vector<int> ans;
//  for (int i = 0; i < n; i++) {
//    tree.GetValue(i, ans);
//    ASSERT_EQ(kv_map[i], ans[i]);
//  }
//  ASSERT_TRUE(tree.Check());
//}
//TEST(BPlusTreeTests,DISABLED_random_Insert) {
//  // Init engine
//  DBStorageEngine engine(db_name);
//  BasicComparator<int> comparator;
//  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//  TreeFileManagers mgr("tree_");
//  // Prepare data
//  const int n = 50;
//  vector<int> keys;
//  vector<int> values;
//  vector<int> delete_seq;
//  map<int, int> kv_map;
//  for (int times = 0; times <= 500; times++) {
//    keys.clear();
//    values.clear();
//    delete_seq.clear();
//    kv_map.clear();
//
//    for (int i = n - 1; i >= 0; i--) {
//      keys.push_back(i);
//      values.push_back(i);
//      delete_seq.push_back(i);
//    }
//    // Shuffle data
//    ShuffleArray(keys);
//    ShuffleArray(values);
//    ShuffleArray(delete_seq);
//    // Map key value
//    for (int i = 0; i < n; i++) {
//      kv_map[keys[i]] = values[i];
//    }
//    // Insert data
//    for (int i = 0; i < n; i++) {
//      //std::cout << i + 1 << " turns: " << keys[i] << " " << values[i] << endl;
//      tree.Insert(keys[i], values[i]);
//      ASSERT_TRUE(tree.Check());
//    }
//
//    // Print tree
//    // Search keys
//    vector<int> ans;
//    for (int i = 0; i < n; i++) {
//      tree.GetValue(i, ans);
//      ASSERT_EQ(kv_map[i], ans[i]);
//    }
//    ASSERT_TRUE(tree.Check());
//    tree.Destroy();
//    ASSERT_TRUE(tree.Check());
//  }
//}
//
//TEST(BPlusTreeTests, DISABLED_asc_DeleteTest) {
//  // Init engine
//  DBStorageEngine engine(db_name);
//  BasicComparator<int> comparator;
//  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//  TreeFileManagers mgr("tree_");
//  // Prepare data
//  const int n = 50;
//  vector<int> keys;
//  vector<int> values;
//  vector<int> delete_seq;
//  map<int, int> kv_map;
//  for (int i = 0; i < n; i++) {
//    keys.push_back(i);
//    values.push_back(i);
//    delete_seq.push_back(i);
//  }
//
//  // Insert data
//  for (int i = 0; i < n; i++) {
//    tree.Insert(keys[i], values[i]);
//  }
//  ASSERT_TRUE(tree.Check());
//  // Print tree
//
//  // Delete half keys
//  for (int i = 0; i < n; i++) {
//    tree.Remove(delete_seq[i]);
//    ASSERT_TRUE(tree.Check());
//  }
//}
//
//TEST(BPlusTreeTests, DISABLED_dsc_DeleteTest) {
//  // Init engine
//  DBStorageEngine engine(db_name);
//  BasicComparator<int> comparator;
//  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//  TreeFileManagers mgr("tree_");
//  // Prepare data
//  const int n = 50;
//  vector<int> keys;
//  vector<int> values;
//  vector<int> delete_seq;
//  map<int, int> kv_map;
//  for (int i = 0; i < n; i++) {
//    keys.push_back(i);
//    values.push_back(i);
//    delete_seq.push_back(i);
//  }
//
//  // Insert data
//  for (int i = 0; i < n; i++) {
//    tree.Insert(keys[i], values[i]);
//  }
//  ASSERT_TRUE(tree.Check());
//  // Print tree
//  //tree.PrintTree(mgr[0]);
//
//  // Delete half keys
//  for (int i = n - 1; i >= 0; i--) {
//    tree.Remove(delete_seq[i]);
//    ASSERT_TRUE(tree.Check());
//   // tree.PrintTree(mgr[1]);
//  }
// /// tree.PrintTree(mgr[1]);
//}
//
//TEST(BPlusTreeTests, DISABLED_random_DeleteTest) {
//  // Init engine
//  DBStorageEngine engine(db_name);
//  BasicComparator<int> comparator;
//  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//  TreeFileManagers mgr("tree_");
//  // Prepare data
//  const int n = 20;
//  vector<int> keys;
//  vector<int> values;
//  vector<int> delete_seq;
//  map<int, int> kv_map;
//  for (int i = 0; i < n; i++) {
//    keys.push_back(i);
//    values.push_back(i);
//    delete_seq.push_back(i);
//  }
//  const int N = 1000;
//  for (int times = 0; times < N; times++) {
//    // Insert data
//   // mgr[0] << times + 1 << " times-turns " << endl;
//    for (int i = 0; i < n; i++) {
//      //mgr[0] << i + 1 << " turns: " << keys[i] << " " << values[i] << endl; 
//     
//     // cout << i + 1 << " turns: " << keys[i] << " " << values[i] << endl; 
//     
//      tree.Insert(keys[i], values[i]);
//      ASSERT_TRUE(tree.Check());
//      
//    }
//      
//    
//    // Print tree
//    //tree.PrintTree(mgr[0]);
//    ASSERT_TRUE(tree.Check());
//    ShuffleArray(delete_seq);
//    //4 3 2 8 5 6 9 0 7 1
//    //Debug
//    //delete_seq[0] = 4;
//    //delete_seq[1] = 3;
//    //delete_seq[2] = 2; 
//    //delete_seq[3] = 8;
//    //delete_seq[4] = 5;  
//    //delete_seq[5] = 6;
//    //delete_seq[6] = 9;
//    //delete_seq[7] = 0;
//    //delete_seq[8] = 7;
//    ////  Above Sequence are all correct
//    ////  Current Position
//    //delete_seq[9] = 1;
//
//  
//    
//      for (int i = 0; i < n / 2; i++) 
//      {
//           // mgr[0] << "Delete---" << i + 1 << " turns: " << delete_seq[i] << endl;
//            //cout << "Delete---" << i + 1 << " turns: " << delete_seq[i] << endl;
//            if (i ==9) {
//              tree.Remove(delete_seq[i]);
//              //tree.PrintTree(mgr[0]);
//          
//            }
//            else {
//            tree.Remove(delete_seq[i]);
//            //tree.PrintTree(mgr[0]);
//            }
//            ASSERT_TRUE(tree.Check());
//      }
//     
//        //tree.PrintTree(mgr[0]);
//      
//
//      tree.Destroy();
//      ASSERT_TRUE(tree.Check());
//    
//   
//  }
//  cout << "fuck" << endl;
//
//}
//
//TEST(BPlusTreeTests, DISABLED_SampleTest) {
//  // Init engine
//  DBStorageEngine engine(db_name);
//  BasicComparator<int> comparator;
//  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//  TreeFileManagers mgr("tree_");
//  // Prepare data
//  const int n = 500;
//  vector<int> keys;
//  vector<int> values;
//  vector<int> delete_seq;
//  map<int, int> kv_map;
//  
//  
//  for (int i = 0; i < n; i++) {
//    keys.push_back(i);
//    values.push_back(i);
//    delete_seq.push_back(i);
//  }
//
// 
//    // Shuffle data
//    ShuffleArray(keys);
//    ShuffleArray(values);
//    ShuffleArray(delete_seq);
//    // Map key value
//    for (int i = 0; i < n; i++) {
//      kv_map[keys[i]] = values[i];
//    }
//    // Insert data
//    for (int i = 0; i < n; i++) {
//      cout << i + 1 << " insert-turns" << keys[i] <<endl;
//      mgr[0]  << keys[i] << " " << values[i] << endl;
//      tree.Insert(keys[i], values[i]);
//      // tree.PrintTree(mgr[i]);
//    }
//    ASSERT_TRUE(tree.Check());
//    // Print tree
//    // Search keys
//    vector<int> ans;
//    for (int i = 0; i < n; i++) {
//      tree.GetValue(i, ans);
//      ASSERT_EQ(kv_map[i], ans[i]);
//    }
//    ASSERT_TRUE(tree.Check());
//    // Delete half keys
//    for (int i = 0; i < n / 2; i++) {
//      cout << i + 1 << " Delete-turns" << delete_seq[i] << endl;
//      mgr[0] <<delete_seq[i] << endl;
//      tree.Remove(delete_seq[i]);
//    }
//    // Check valid
//    ans.clear();
//    for (int i = 0; i < n / 2; i++) {
//      ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
//    }
//    for (int i = n / 2; i < n; i++) {
//      ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
//      ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
//    }
//    tree.Destroy();
//    ASSERT_TRUE(tree.Check());
//  
//
//}
//TEST(BPlusTreeTests, SampleTest) {
//  // Init engine
//  DBStorageEngine engine(db_name);
//  BasicComparator<int> comparator;
//  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, 4, 4);
//  TreeFileManagers mgr("tree_");
//  // Prepare data
//  const int n = 30;
//  vector<int> keys;
//  vector<int> values;
//  vector<int> delete_seq;
//  map<int, int> kv_map;
//  for (int i = 0; i < n; i++) {
//    keys.push_back(i);
//    values.push_back(i);
//    delete_seq.push_back(i);
//  }
//  // Shuffle data
//  ShuffleArray(keys);
//  ShuffleArray(values);
//  ShuffleArray(delete_seq);
//  // Map key value
//  for (int i = 0; i < n; i++) {
//    kv_map[keys[i]] = values[i];
//  }
//  // Insert data
//  for (int i = 0; i < n; i++) {
//    tree.Insert(keys[i], values[i]);
//  }
//  ASSERT_TRUE(tree.Check());
//  // Print tree
//  tree.PrintTree(mgr[0]);
//  // Search keys
//  vector<int> ans;
//  for (int i = 0; i < n; i++) {
//    tree.GetValue(i, ans);
//    ASSERT_EQ(kv_map[i], ans[i]);
//  }
//  ASSERT_TRUE(tree.Check());
//  // Delete half keys
//  for (int i = 0; i < n / 2; i++) {
//    tree.Remove(delete_seq[i]);
//  }
//  /*or (int i = 0; i < n ; i++) {
//    tree.Remove(delete_seq[i]);
//  }
//  ASSERT_TRUE(tree.Check());*/
//
//  tree.PrintTree(mgr[1]);
//  // Check valid
//  ans.clear();
//  for (int i = 0; i < n / 2; i++) {
//    ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
//  }
//  for (int i = n / 2; i < n; i++) {
//    ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
//    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
//  }
//}