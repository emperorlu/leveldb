#include <iostream>
#include <fstream>
#include <memory.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
using namespace std;
void PrintBuffer(const void* pBuff, unsigned int nLen)
{
    if (NULL == pBuff || 0 == nLen) {
        return;
    }

    const int nBytePerLine = 16;
    unsigned char* p = (unsigned char*)pBuff;
    char szHex[3*nBytePerLine+1] = {0};

    printf("-----------------begin-------------------\n");
    for (unsigned int i=0; i<nLen; ++i) {
        int idx = 3 * (i % nBytePerLine);
        if (0 == idx) {
            memset(szHex, 0, sizeof(szHex));
        }
        snprintf(&szHex[idx], 4, "%02x ", p[i]); // buff长度要多传入1个字节
        
        // 以16个字节为一行，进行打印
        if (0 == ((i+1) % nBytePerLine)) {
          printf("%s\n", szHex);
        }
    }

    // 打印最后一行未满16个字节的内容
    if (0 != (nLen % nBytePerLine)) {
        printf("%s\n", szHex);
    }
    printf("------------------end-------------------\n");
}
int main(){
    //ifstream infile;
    char buf[1024]; 
    
    // fstream infile ("/tmp/leveldbtest-0/dbbench/000005.ldb", ios::in);
    // //infile.open("/tmp/leveldbtest-0/dbbench/000005.ldb");
    // // if(infile.is_open()){
    // //     while(infile.good() && !infile.eof()){
    // //     memset(buf,0,1024);
    // //     // infile.getline(buf,1204);
    // //     // message = buf;
    // //     // cout<<message<<endl;
    // //     infile.seekp(1808305,ios::beg);
    // //     infile >> buf; 
    // //     cout << buf << endl; 
    // //     }
    // // }
    // memset(buf,0,1024);
        
    // infile.seekp(1808305,ios::beg);
    // infile >> buf; 
    // cout << buf << endl; 
    // infile.close();

    int fd;
    int count = 225;
    int offset = 1808305;
    int ret;
    fd = open( "/tmp/leveldbtest-0/dbbench/000005.ldb", O_RDONLY);
    ret =pread(fd,buf,count,offset);
    printf("the read data is:%s\n", buf);
    PrintBuffer(buf, 225);
    close(fd);
}