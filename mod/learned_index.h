//
// Created by daiyi on 2020/02/02.
//

#ifndef LEVELDB_LEARNED_INDEX_H
#define LEVELDB_LEARNED_INDEX_H


#include <vector>
#include <cstring>
#include <atomic>
#include "plr.h"



using std::string;

namespace adgMod {
    class LearnedIndex {

     private:
        double error;
     public:

        std::vector<Segment> string_segments;
        std::vector<char> based_char;
        std::vector<double> based_num;
        // std::string param;
        char* param;
        int max_lenth;
        // double min_key;
        // double max_key;
        uint64_t size;



     public:
        std::vector<std::string> string_keys;
        uint64_t block_num_entries;
        uint64_t block_size;
        uint64_t entry_size;
        explicit LearnedIndex(): error(0), max_lenth(0){
            param = new char[4096];
        };
        void FileLearn();
        std::vector<double> toCode();
    };

}

#endif //LEVELDB_LEARNED_INDEX_H
