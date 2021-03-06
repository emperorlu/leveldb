// #include <gtest/gtest.h>

#include "learned_index.h"
#include <iostream>
// #include "../src/datastream/rocksdb_stream.hpp"
using namespace std;
int main(){

  RMIConfig rmi_config;
  RMIConfig::StageConfig first, second;

  first.model_type = RMIConfig::StageConfig::LinearRegression;
  first.model_n = 1;

  second.model_n = 1000;
  second.model_type = RMIConfig::StageConfig::LinearRegression;
  rmi_config.stage_configs.push_back(first);
  rmi_config.stage_configs.push_back(second);

  LearnedRangeIndexSingleKey<uint64_t,float> table(rmi_config);
  // RocksStream<uint64_t,uint64_t> db("./testdb");
  // auto it = db.get_iter();

  // int count = 0;
  // for(it->begin();it->valid();it->next()) {
  //   auto key = it->key();
  //   auto value = it->value();
  //   LOG(4) << key;
  //   table.insert(key,value);
  //   count++;
  // }
  srand((unsigned)time(NULL)); 
  vector<double> x;
  vector<double> y;
  // vector<double> z;
  double key = 0;
  double value = 0;
  int size = 160000;
  cout << "key_size: " << size << "* 2* sizeof(double) = " << size * 2 * sizeof(double) << endl;
  for (int i = 0; i < size; i++){
    key += rand()%100+1;
    value += rand()%100+1;
    // cout << "train:" << key << ": " << value << endl;
    table.insert(key,value);
    // count++;
    x.push_back(key);
    y.push_back(value);
  }
  // ASSERT_EQ(count,12);
  table.finish_insert();
  table.finish_train();

  // LOG(4) << "finished insert";

  for (int i = 0; i < size; i++){
    key = x[i];
    value = y[i];
    // cout << "get: " << key << ": " << value << endl;
    auto value_get = table.get(key);
    // cout << endl;
    if (value_get != value) cout << __func__ << " " << i << "find error!" << endl;
    // cout << " value_get:" << value_get << endl;
  }
  table.printR();
  // it->begin();
  // for(it->begin();it->valid();it->next()) {
  //   auto key = it->key();
  //   auto value = table.get(key);
  //   ASSERT_EQ(value,it->value());
  // }

} // end namespace test
