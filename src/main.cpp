#include<stdio.h>
#include<stdlib.h>
#include<BST.h>
#include<KVDB.h>
#include<string>
#include<iostream>
#include<cstring>
#include<fstream>
#include "masstree_wrapper.h"
#include <iostream>
#include <string>


int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <num_threads> <num_operations> <compaction>\n";
        return 1;
    }

    int num_threads = std::atoi(argv[1]);
    int num_operations = std::atoi(argv[2]) * 1000000;  // 转换为百万次
    int compaction = std::atoi(argv[3]);
    KVDB* kvdb = new KVDB(compaction);
    string out=test(kvdb,num_threads, num_operations, 123456789);
  
    delete kvdb;
    std::ofstream file("test_results.txt",std::ios::app);
    file << "线程数: " << num_threads << ", 操作数: " << num_operations << "\n" << "limit_count: 10M\n";
    file << out << "\n\n\n";
    file.close();



    std::cout<<out<<endl;
    

    return 0;

    

    // std::vector<int> thread_counts = {2, 4, 8, 16, 24, 32};
    // std::vector<int> operation_counts = {20, 30, 40, 50, 60, 70, 80, 90, 100};

    // for (int thread_count : thread_counts) {
    //     for (int operation_count : operation_counts) {
    //         operation_count *= 1000000;  // 转换为百万次
    //         for (int i = 0; i < 10; ++i) {  // 进行10次测试
    //             KVDB* kvdb = new KVDB();
    //             std::string result = test(kvdb, thread_count, operation_count, 123456789);
                
    //             std::ofstream file("test_results.txt",std::ios::app);
    //             file << "Thread count: " << thread_count << ", Operation count: " << operation_count << "\n";
    //             file << result << "\n\n\n";
    //             file.close();
    //             delete kvdb;
    //             std::this_thread::sleep_for(std::chrono::seconds(10));
    //             cout<<"finish  this test"<<endl;
    //         }
    //     }
    // }

    
    return 0;
}