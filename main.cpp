#include <iostream>
#include "wal.h"
const char filename[] = "/home/user/testDev/test";
const char test[] = "test";
int main() {
    int fd = open(filename,O_CREAT|O_RDWR,0666);
    int count = 0;
    for (int i = 0; i < 10000;i++) {
        auto res = write(fd,test,sizeof(test));
        if (res != sizeof(test)) {
            printf("error happened\n");
            count++;
        }else {
            // printf("write success\n");
        }
        usleep(1000);
    }
    printf("error happened have %d times\n",count);;

    close(fd);
    return 0;
}