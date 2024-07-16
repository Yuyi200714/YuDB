#include"KVDB.h"
#include<string>
#include<fstream>
#include<vector>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <filesystem>
extern BufferIndex bufferIndex;
void KVDB::loadFromFile(){
}

void KVDB::Flush(){
    std::chrono::high_resolution_clock::time_point start, end;
    double serialize_time, write_time;
    start = std::chrono::high_resolution_clock::now();
    this->serialize();
    end = std::chrono::high_resolution_clock::now();
    serialize_time = std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();

    start = std::chrono::high_resolution_clock::now();
    if(this->compaction==1){
        data_test<<bufferIndex.compaction();
    }
    else{
        bufferIndex.flush();
    }
    end = std::chrono::high_resolution_clock::now();
    write_time = std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();

    std::cout << "将masstree转化为缓冲数据块的时间: " << serialize_time << " s\n";
    std::cout << "缓冲数据块compaction和落盘总时间 " << write_time << " s\n";
    data_test << "将masstree转化为缓冲数据块的时间: " << serialize_time << " s\n";
    data_test << "缓冲数据块compaction和落盘总时间 " << write_time << " s\n";
}

KVDB::KVDB(int compaction):compaction(compaction){
    this->mt = new MasstreeWrapper();
    this->mt->table_init();
    this->count=0;
    this->flush_num = 0;
    this->Bgt = std::thread(&KVDB::BgtTask, this);

}
    
KVDB::~KVDB(){
    //read_test(this,100);

    this->is_closing = true;
    this->stopThread = true;
    this->Bgt.join();
    this->Edt = std::thread(&KVDB::EdtTask, this);
    string value;
    get("key999",value);
    std::cout<<value<<endl;
    this->ending = true;

    this->Edt.join();

    for (const auto& entry : std::filesystem::directory_iterator("/mnt/intel/zyb/")) {
        if (entry.is_regular_file()) {
            std::filesystem::remove(entry.path());
        }
    }
    std::ofstream file("compaction_results.txt",std::ios::app);
    file << data_test.str() << "\n\n\n";
    file.close();
}
void KVDB::put(string key, uint64_t value){
    this->count++;
    this->mt->insert(key.c_str(), value);
}
void KVDB::del(string key){
    std::ofstream logFile;
    //logFile.open(this->logFilename, std::ios::out | std::ios::app);
    logFile << "del " << key << std::endl;
    this->count--;
    //logFile.close();
    //this->bst->Erase(key);
    this->mt->remove(key.c_str());
}
bool KVDB::get(string key,string &value){
    uint64_t temp_value;
  // 先从mt里找
    if (this->mt != nullptr && this->mt->search(key.c_str(), temp_value)) {
        value = std::to_string(temp_value);
        return true;
    }

    // 再从oldmt里找

    // 最后从bufferindex里找
    if (bufferIndex.get(key.c_str(), value)) {
        return true;
    }

    // 如果都没有找到，返回false
    return false;
}

void KVDB::serialize(){
    //负责将内存索引数据转化到缓冲数据块中
    std::vector<std::pair<std::string, uint64_t>> vec;
    this->old_mt->global_scan(this->old_count, this->flush_num, vec);
    // // 打开文件
    // std::ofstream file("/mnt/intel/zyb/data.txt");

    // // 将vec中的数据写入文件
    // for (const auto& pair : vec) {
    //     file << pair.first << " " << pair.second << "\n";
    // }

    // // 关闭文件
    // file.close();
    return ;
}

void KVDB::deserialize(string data){
    this->bst->deserialize(data);
}

void KVDB::BgtTask(){
    while(!this->stopThread){
        if(count >= this->limit_count){
            this->mt_live = false;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            cout<<endl<<"------------------------------------------------------"<<endl;
            cout<<"进行第"<<this->flush_num+1<<"次flush"<<endl;
            cout<<"本次flush的键值对数量为"<<this->count<<endl;
            data_test<<"本次flush的键值对数量为"<<this->count<<endl;
            auto start = std::chrono::high_resolution_clock::now();
            // while(!this->is_flushing){//如果正在写入磁盘，等待
            //     std::this_thread::sleep_for(std::chrono::milliseconds(10));
            // }
            this->old_mt = this->mt;
            this->mt = new MasstreeWrapper();//第一次新建的时候耗时比较久
            this->old_count = this->count;
            this->count = 0;
            this->mt_live = true;
            this->Flush();
            
            delete this->old_mt;
            this->old_count = 0;
            
            this->flush_num++;
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> diff = end - start;
            std::cout << "本次flush所需时间: " << diff.count() << "s\n";
            data_test << "本次flush所需时间: " << diff.count() << "s\n";
            cout<<"------------------------------------------------------"<<endl;
        }
        if(this->is_closing){
            // cout<<endl<<"------------------------------------------------------"<<endl;
            // cout<<"进行第"<<this->flush_num+1<<"次flush，也是最后一次"<<endl;
            // cout<<"本次flush的键值对数量为"<<this->count<<endl;
            // auto start = std::chrono::high_resolution_clock::now();
            // this->old_mt = this->mt;
            // this->old_count = this->count;
            // this->Flush();
            // delete this->old_mt;
            // this->old_count = 0;
            // auto end = std::chrono::high_resolution_clock::now();
            // std::chrono::duration<double> diff = end - start;
            // std::cout << "本次flush所需时间: " << diff.count() << "s\n";
            // cout<<"------------------------------------------------------"<<endl;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

void KVDB::EdtTask(){
    while(1){
        if(this->ending){
            cout<<endl<<"------------------------------------------------------"<<endl;
            cout<<"进行第"<<this->flush_num+1<<"次flush，也是最后一次"<<endl;
            cout<<"本次flush的键值对数量为"<<this->count<<endl;
            data_test<<"本次flush的键值对数量为"<<this->count<<endl;
            auto start = std::chrono::high_resolution_clock::now();
            this->old_mt = this->mt;
            this->old_count = this->count;
            this->Flush();
            delete this->old_mt;
            this->old_count = 0;
            auto end = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> diff = end - start;
            std::cout << "本次flush所需时间: " << diff.count() << "s\n";
            cout<<"------------------------------------------------------"<<endl;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

void insert_operation(KVDB* kvdb, int start, int end, int thread_id) {
    MasstreeWrapper::thread_init(thread_id);
    std::thread::id tid = std::this_thread::get_id();
    std::stringstream os;
    os<<tid;
    // if(kvdb->mt_live==false){
    //     exit(0);
    // }
    std::ofstream logFile("/mnt/intel/zyb/log_" + os.str() + ".txt", std::ios::app);
    for (int i = start; i < end; ++i) {
        std::string key = "key" + std::to_string(i);
        uint64_t value = i;
        logFile << "put " + key + " " + std::to_string(value) + "\n";
        while(!kvdb->mt_live){
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        kvdb->put(key, value);
    }
    logFile.close();
}

std::string test(KVDB* kvdb,int thread_num, int op_num, uint64_t value){
    std::vector<std::thread> threads;
    auto start_time = std::chrono::high_resolution_clock::now();
    op_num = op_num / thread_num;
    for(int i = 0; i < thread_num; i++){
        threads.push_back(std::thread(insert_operation, kvdb, i * op_num, (i + 1) * op_num, i));
    }
    for(auto& thread : threads){
        thread.join();
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end_time - start_time;
    int op=thread_num*op_num/1000000;
    std::ostringstream output;
    output << "线程数：" << thread_num << "\n";
    output << "Time to op " << op << "M operations: " << diff.count() << "s\n";
    output << "吞吐率："<<op/diff.count()  << "MOPS\n";

    
    return output.str();
}

std::string read_test(KVDB*kvdb,int read_num){
    // 在键范围内生成1M个键
    std::vector<std::string> keys;
    for (int i = 0; i < read_num; ++i) {
        keys.push_back("key" + std::to_string(i));
    }

    // 调用 put 接口进行查询，记录每次操作的延迟
    std::vector<double> latencies;
    std::string test_value;
    for (const auto& key : keys) {
        auto start_time = std::chrono::high_resolution_clock::now();
        kvdb->get(key, test_value);
        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> latency = end_time - start_time;
        latencies.push_back(latency.count());
    }

    // 计算平均延迟
    double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
    double average_latency = sum / latencies.size();
    std::ostringstream output;
    output << "平均延迟: " << average_latency << "s\n";

    // 计算99分位尾延迟
    std::sort(latencies.begin(), latencies.end());
    double percentile_99_latency = latencies[latencies.size() * 0.99];
    output << "99分位尾延迟: " << percentile_99_latency << "s\n";
    return output.str();
}