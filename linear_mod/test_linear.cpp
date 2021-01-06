#include <iostream>
#include "linear.h"

using namespace mod;

int main(){
    std::vector<u64> x = {1,2,44,53,421};
    std::vector<u64> y = {3,23,444,553,4432};
    
    // for (int i = 0; i < )
    mod::LRModel lr = new LRModel;
    lr.train(x,y,10);
    std::vector<u64> z;
    for(int i = 0; i < x.size(); i++){
        z[i] = lr.predict(x[i]);
        cout << z[i] << endl;
    }
    // predict(44);

}