//
// Created by daiyi on 2020/02/02.
//

#include <cstdint>
#include <cassert>
#include <utility>
#include <cmath>
#include <iostream>
#include <fstream>
#include "util/mutexlock.h"
#include "learned_index.h"
// #include "util.h"
#include "db/version_set.h"

#include <sstream>

namespace adgMod {
    uint64_t block_num_entries = 0;
    uint64_t block_size = 0;
    uint64_t entry_size = 0;

    std::vector<double> LearnedIndex::toCode(){
        
        double pre_ba;
        double based = 1;

        std::vector<double> turn;
        
        for (int i = 0; i < string_keys.size(); i++){
            reverse(string_keys[i].begin(),string_keys[i].end());
            if (string_keys[i].length() > max_lenth) max_lenth = string_keys[i].length();
        }
        for (int i = 0; i < max_lenth; i++){
            std::vector<char> compare;
            for (int j = 0; j < string_keys.size(); j++) {
                compare.push_back(string_keys[j][i]);
            }
            std::vector<char>::iterator max = max_element(compare.begin(), compare.end());
            std::vector<char>::iterator min = min_element(compare.begin(), compare.end());
            based_char.push_back(*min);

            if (i != 0) based *= pre_ba;
            pre_ba = *max-*min+1;
            based_num.push_back(based);
        }
        for (int i = 0; i < string_keys.size(); i++){
            double num = 0;
            for (int j = 0; j < string_keys[i].length(); j++){
                num += based_num[j] * (double)(string_keys[i][j] - based_char[j]);
            }
            
            turn.push_back(num);
        }
        string_keys.clear();
        return turn;
    }

    void LearnedIndex::FileLearn() {
        // FILL IN GAMMA (error)
        PLR plr = PLR(error);

        if (string_keys.empty()) assert(false);
        
        std::vector<double> double_key = this->toCode();

        double temp = double_key.back();
        //min_key = double_key.front();
        //max_key = double_key.back();
        size = string_keys.size();

        //std::vector<Segment> segs = plr.train(string_keys, !is_level);
        std::vector<Segment> segs = plr.train(double_key, true);

        if (segs.empty()) return;
        segs.push_back((Segment) {temp, 0, 0, 0});
        string_segments = std::move(segs);

        // for (auto& str: string_segments) {
        //     //printf("%s %f\n", str.first.c_str(), str.second);
        // }

        // learned.store(true);
        //string_keys.clear(); 
        std::stringstream stream;
        stream << adgMod::block_num_entries << " " << adgMod::block_size << " " << adgMod::entry_size << " ";
        for (Segment& item: string_segments) {
            stream << item.x << " " << item.k << " " << item.b << " " << item.x2 << " ";
        }
        for (char item: based_char){
            stream << item << " ";
        }
        for (double item: based_num){
            stream << item << " ";
        } 
        stream >> param;
        std::cout << __func__ << " param: " << param << std::endl;
    }

}
