// #include <gtest/gtest.h>

#include "learned_index.h"
#include <iostream>
// #include "../src/datastream/rocksdb_stream.hpp"
using namespace std;
int main(int argc,char *argv[]){

  RMIConfig rmi_config;
  RMIConfig::StageConfig first, second;

  // XD: which is first, and which is second means?
  // does this means that there are only 2 stages for the model?
  // pps: seems my guess is correct
  first.model_type = RMIConfig::StageConfig::LinearRegression;
  first.model_n = 1;

  second.model_n = atoi(argv[1]);
  second.model_type = RMIConfig::StageConfig::LinearRegression;
  rmi_config.stage_configs.push_back(first);
  rmi_config.stage_configs.push_back(second);

  LearnedRangeIndexSingleKey<uint64_t,float> table(rmi_config);

  srand((unsigned)time(NULL)); 
  vector<double> x;
  vector<double> y;
  // vector<double> z;
  double key = 0;
  double value = 0;

  std::ifstream input_file("result.txt");

	while (true) {
    if (!(input_file >> value)) break;
    key += rand()%100+1;
    // cout << "train:" << key << ": " << value << endl;
    table.insert(key,value);
    // count++;
    x.push_back(key);
    y.push_back(value);
  }

  table.finish_insert();
  table.finish_train();

  vector<int> result(15000);
  for (int i = 0; i < x.size(); i++){
    key = x[i];
    value = y[i];
    // cout << "get: " << key << ": " << value << endl;
    auto value_get = table.get(key);
    double bit = 1.0* (value-value_get) / value;
    int block = value_get / 4096;
    cout << i << " result: " << value_get << " : " << value << "; block:" << block <<
          "; error:" << (value-value_get)  << ";error bit: "<< bit << endl;
    result[block]++;
  }
  for (int i = 0; i < result.size(); i++){
    if (result[i] != 0)
      cout << i << " block_num: " << result[i] << endl;
  }

  table.printR();


} // end namespace test
