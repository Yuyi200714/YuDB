#pragma once
#include<BST.h>
#include<string>
#include<chrono>
#include<fstream>
#include<masstree_wrapper.h>
#include<mutex>
#include<atomic>
#include<thread>
#include<Buffer.h>
using namespace std;
class KVDB{
    typedef BSTNode Node;
    private:
        BST *bst;
        std::mutex mtx;
        std::atomic<int> count;
        int old_count;
        string filename = "test.db";
        string logFilename = "log.txt";
        string  path = "/mnt/intel/zyb/";
        fstream logFile;
        chrono::duration<double> totalTime;
        MasstreeWrapper *mt;
        MasstreeWrapper *old_mt;
        void loadFromFile();
        void Flush();
        const int limit_count=1000000;//1M个
        std::thread Bgt;//后台线程
        std::thread Edt;//收尾线程
        bool stopThread = false;
        int compaction;
        std::ostringstream data_test;

    public:
        bool mt_live = true;
        bool is_closing = false;
        bool is_flushing = false;
        bool ending = false;
        int flush_num;
        KVDB(int compaction);
        ~KVDB();
        void put(string key, uint64_t value);
        void del(string key);
        bool get(string key,string &value);
        void serialize();
        void deserialize(string data);
        void BgtTask();
        void EdtTask();

};
void insert_operation(KVDB* kvdb, int start, int end, int thread_id);

string test(KVDB* kvdb,int thread_num, int op_num, uint64_t value);

string read_test(KVDB* kvdb,int read_num);
