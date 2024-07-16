#pragma once
#include<string>
#include<vector>
#include<fstream>
#include<map>

class KeyValuePair {
public:
    std::string key;
    std::string value;

    KeyValuePair(const std::string& key, const std::string& value)
    :key(key),value(value){}

    KeyValuePair(){}
};

class IndexBlock {
public:
    std::string maxKey[15];
    int offset[15];
    int kv_num[15];

    IndexBlock() {offset[0]=0; kv_num[0]=0;}
};

class BufferBlock {
private:
    std::vector<KeyValuePair> dataBlocks[15]; // 数据块
    IndexBlock indexBlock; // 索引块
    int currentBlock; // 当前正在使用的数据块的索引
    int id;
    
    int block_size[15];
    std::string minKey;
    std::string maxKey;
    int getKVF;
    int getKVS;

public:
    int seq; // 第几次刷新得到的
    BufferBlock(int id, int seq);
    std::string getMinKey() const;
    std::string getMaxKey() const;
    int getSeq() const;
    int getId() const;
    bool add(const KeyValuePair& kvp);
    const std::vector<KeyValuePair>& getDataBlocks(int index) const;
    const IndexBlock& getIndexBlock() const;
    bool serial_get(KeyValuePair& kvp);
    bool get(std::string key,std::string& value);
    void writeToSST();
};

class BufferIndex {
public:
    struct Manifest {
        int id;
        std::string minKey;
        std::string maxKey;
        BufferBlock* bufferBlockPtr;
        bool isWrittenToSST;
        int seq;

        Manifest(int id, const std::string& minKey, const std::string& maxKey, BufferBlock* bufferBlockPtr, bool isWrittenToSST, int seq)
        : id(id), minKey(minKey), maxKey(maxKey), bufferBlockPtr(bufferBlockPtr), isWrittenToSST(isWrittenToSST), seq(seq){}
    };

    struct Compare {
        bool operator()(const std::pair<std::string, std::string>& a, const std::pair<std::string, std::string>& b) const{
            if (a.first < b.first) {
                return true;
            }
            if (a.first == b.first && a.second < b.second) {
                return true;
            }
            return false;
        }
    };

    std::map<std::pair<std::string, std::string>, Manifest, Compare> indexMap;

    void add(BufferBlock* bufferBlockPtr);
    void haveWritten(BufferBlock* bufferBlockPtr);
    void flush();
    std::string compaction();
    bool get(const std::string& key, std::string& value);
};





