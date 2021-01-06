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
#include <iterator>
#include <sstream>

namespace adgMod {
    uint64_t block_num_entries = 0;
    uint64_t block_size = 0;
    uint64_t entry_size = 0;

    std::vector<double> LearnedIndex::toCode(){
        
        double pre_ba;
        double based = 1;

        std::vector<double> turn;
        // std::cout << __func__ << " size: " << string_keys.size() << std::endl;
        for (int i = 0; i < string_keys.size(); i++){
            // std::cout << __func__ << " i: " << i << "; string_keys: " << string_keys[i] << std::endl;
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
        // std::cout << __func__ << " size: " << size << std::endl;
        // for(int i = 0; i < string_keys.size(); i++){
        //     std::cout << __func__ << " i: " << i << "; string_keys: " << string_keys[i] << std::endl;
        // }
        // std::cout << __func__ << " max_leath: " << max_lenth << std::endl;
        // for(int i = 0; i < max_lenth; i++){
        //     std::cout << __func__ << " i: " << i << "; based_num: " << based_num[i] << std::endl;
        //     std::cout << __func__ << " i: " << i << "; based_char: " << based_char[i] << std::endl;
        //     std::cout << __func__ << " i: " << i << "; double_key: " << double_key[i] << std::endl;
        // }


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
        string stemp;
        // stream << max_lenth << ",";
        stream << max_lenth;
        // stream >> stemp;
        // param += stemp;
        // for (char item: based_char){
        //     std::cout << "item in based_char: " << item << "; "  << std::endl;
        //     stream << item;
        // }
        // for (double item: based_num){
        //     std::cout << "item in based_num: " << item << std::endl;
        //     stream << item << " ";
            
        // } 
        std::copy(based_char.begin(), based_char.end(), std::ostream_iterator<char>(stream));
        std::copy(based_num.begin(), based_num.end(), std::ostream_iterator<double>(stream,","));

        /*
        stream << adgMod::block_num_entries << "," << adgMod::block_size << "," << adgMod::entry_size << ",";
        // std::copy(based_num.begin(), based_num.end(), std::ostream_iterator<double>(stream,","));
        for (Segment& item: string_segments){
            stream << item.x << "," << item.k << "," << item.b << "," << item.x2 << ",";
        }
        
        std::cout << __func__ << " param size:" << param.length() << " ;param: " << param << std::endl;

        std::stringstream stream2;
        // based_char.clear();
        // based_num.clear();
        
        stream2 << param;
        double max_len;
        stream2 >> max_len;
        std::cout << "max_len: " << max_len << std::endl;
        char tmpc;
        for (char item: based_char){
            char item2;
            stream2 >> item2;
            // char item3 = item2+'0';
            std::cout << "item in based_char: " << item << std::endl;
            std::cout << "return!!! item in based_char: " << item2 << std::endl;
        }
        for (double item: based_num){
            double item2;
            stream2 >> item2;
            std::cout << "item in based_num: " << item << std::endl;
            std::cout << "return!!! item in based_num: " << item2 << std::endl;
            stream2 >> tmpc;
        }
        int x1,x2,x3;
        stream2 >> x1;
        stream2 >> tmpc;
        stream2 >> x2;
        stream2 >> tmpc;
        stream2 >> x3;
        stream2 >> tmpc;
        stream << adgMod::block_num_entries << "," << adgMod::block_size << "," << adgMod::entry_size << ",";
        for (Segment& item: string_segments){
            stream << item.x << "," << item.k << "," << item.b << "," << item.x2 << ",";
        }
        
        param="";*/
        stream >> param;
        std::cout << __func__ << " param size:" << param.length() << " ;param: " << param << std::endl;
        string_segments.clear();
        based_char.clear();
        based_num.clear();
        string_keys.clear();
    }

}
