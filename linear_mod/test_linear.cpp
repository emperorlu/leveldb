#include <iostream>
#include "linear.h"

using namespace mod;

int main(){
    std::vector<u64> x = {1,2,44,53,421};
    std::vector<u64> y = {64, 64*2, 64*3, 64*4, 64*5};
    
    // for (int i = 0; i < )
    mod::LRModel<> lr;
    std::cout << " train before"<< std::endl;
    lr.train(x,y,100);
    std::cout << " train end"<< std::endl;
    std::vector<double> z;
    for(int i = 0; i < x.size(); i++){
        // std::cout << " predict before"<< std::endl;
        z.push_back(lr.predict(x[i]));
        // std::cout << " predict end"<< std::endl;
        std::cout << z[i] << std::endl;
    }
    // predict(44);

}