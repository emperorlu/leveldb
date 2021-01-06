#include <iostream>
#include <fstream.h>
using namespace std;

int main(){
    ifstream infile;
    char buf[1024]; 
    infile.open("/tmp/leveldbtest-0/dbbench/000005.ldb");
    if(infile.is_open()){
        while(infile.good() && !infile.eof()){
        memset(buf,0,1024);
        // infile.getline(buf,1204);
        // message = buf;
        // cout<<message<<endl;
        infile >> buf; 
        cout << buf << endl; 
        }
    }
    infile.close();
}