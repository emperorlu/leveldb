#include <iostream>
#include <fstream>
#include <memory.h>
using namespace std;

int main(){
    //ifstream infile;
    char buf[225]; 
    fstream infile ("/tmp/leveldbtest-0/dbbench/000005.ldb", ios::in);
    //infile.open("/tmp/leveldbtest-0/dbbench/000005.ldb");
    // if(infile.is_open()){
    //     while(infile.good() && !infile.eof()){
    //     memset(buf,0,1024);
    //     // infile.getline(buf,1204);
    //     // message = buf;
    //     // cout<<message<<endl;
    //     infile.seekp(1808305,ios::beg);
    //     infile >> buf; 
    //     cout << buf << endl; 
    //     }
    // }
    memset(buf,0,225);
        
    infile.seekp(1808305,ios::beg);
    infile >> buf; 
    cout << buf << endl; 
    infile.close();
}