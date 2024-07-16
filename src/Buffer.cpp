#include "Buffer.h"
#include <thread>
#include <iostream>
#include <chrono>
#include <queue>
#include <sstream>
BufferIndex bufferIndex;
int global_id=1;
BufferBlock::BufferBlock(int id, int seq) : currentBlock(0), id(id), seq(seq), getKVF(0), getKVS(0)
    { 
        std::fill_n(block_size, 15, 0);
        std::string filename = "/mnt/intel/zyb/" + std::to_string(id) + ".sst";
        std::ifstream file(filename, std::ios::binary | std::ios::ate);
        if (file.is_open()) {
            std::streamsize size = file.tellg();
            file.seekg(0, std::ios::beg);

            std::vector<char> buffer(size);
            if (file.read(buffer.data(), size)) {
                // Read the index start offset from the last 4 bytes
                int indexStartOffset = *reinterpret_cast<int*>(&buffer[size - 4]);

                // Read the key-value pairs
                for (int i = 0; i < indexStartOffset; i += 40) {
                    KeyValuePair kvp(std::string(buffer.begin() + i, buffer.begin() + i + 20),std::string(buffer.begin() + i + 20, buffer.begin() + i + 40));
                    add(kvp);
                }
            }

            file.close();
            std::remove(filename.c_str());
        }

    }

std::string BufferBlock::getMinKey() const {
    return minKey;
}

std::string BufferBlock::getMaxKey() const {
    return maxKey;
}

int BufferBlock::getSeq() const {
    return seq;
}

int BufferBlock::getId() const {
    return id;
}

bool BufferBlock::serial_get(KeyValuePair& kvp) {
    if (getKVF < 15) {
        if (getKVS < dataBlocks[getKVF].size()) {
            kvp = dataBlocks[getKVF][getKVS];
            getKVS++;
            return true;
        }
        getKVF++;
        getKVS = 0;
        return serial_get(kvp);
    }
    return false;
}

bool BufferBlock::add(const KeyValuePair& kvp) {
    // 检查键和值的长度是否小于或等于 20 字节
    if (kvp.key.size() > 20 || kvp.value.size() > 20) {
        std::cout<<"key:"<<kvp.key<<" value:"<<kvp.value<<std::endl;
        throw std::runtime_error("有键值大于20字节");
    }
    // 如果是第一个键，更新最小键
    if (currentBlock == 0 && dataBlocks[0].empty()) {
        minKey = kvp.key;
    }




    // 计算键值对的大小
    int size = 40;

    // 检查当前数据块是否有足够的空间，每个数据块设为64KB
    if (block_size[currentBlock] + size > 64*1024) {
      

        // 移动到下一个数据块
        currentBlock++;

        // 如果BufferBlock已满，返回false
        if (currentBlock >= 15) {
            return false;
        }

        indexBlock.offset[currentBlock] = indexBlock.offset[currentBlock-1] + indexBlock.kv_num[currentBlock-1]*40;
    }

    // 如果BufferBlock还有空间，添加键值对并返回true
    dataBlocks[currentBlock].push_back(kvp);
    maxKey = kvp.key;
    indexBlock.maxKey[currentBlock]=maxKey;
    indexBlock.kv_num[currentBlock]++;
    block_size[currentBlock] += size;
    return true;
}

const std::vector<KeyValuePair>& BufferBlock::getDataBlocks(int index) const {
    if (index >= 0 && index < 15) {
        return dataBlocks[index];
    }
    // 如果索引无效，返回一个空的vector
    static std::vector<KeyValuePair> empty;
    return empty;
}

const IndexBlock& BufferBlock::getIndexBlock() const {
    return indexBlock;
}

void BufferBlock::writeToSST() {
    std::ofstream file("/mnt/intel/zyb/" + std::to_string(id) + ".sst", std::ios::binary);
    int block_num = std::min(currentBlock+1,15);
    for (int i = 0; i < block_num; i++) {
        for (const auto& kvp : dataBlocks[i]) {
            char key[20];
            std::fill(std::begin(key), std::end(key), '\0');
            std::copy(kvp.key.begin(), kvp.key.end(), key);
            file.write(key, sizeof(key));

            char value[20];
            std::fill(std::begin(value), std::end(value), '\0');
            std::copy(kvp.value.begin(), kvp.value.end(), value);
            file.write(value, sizeof(value));
        }
    }
    int indexOffset = static_cast<int>(file.tellp());
    for (int i = 0; i < this->currentBlock; i++) {
        char maxKey[20];
        std::fill(std::begin(maxKey), std::end(maxKey), '\0');
        std::copy(indexBlock.maxKey[i].begin(), indexBlock.maxKey[i].end(), maxKey);
        file.write(maxKey, sizeof(maxKey));

        int offset = indexBlock.offset[i];
        file.write((char*)&offset, sizeof(offset));

        int kv_num = indexBlock.kv_num[i];
        file.write((char*)&kv_num, sizeof(kv_num));
    }

    file.write((char*)&indexOffset, sizeof(indexOffset));

    file.close();

    bufferIndex.haveWritten(this);
}

bool BufferBlock::get(std::string key,std::string& value) {
    for (int i = 0; i < 15; i++) {
        // 遍历数据块中的所有键值对
        for (const auto& kvp : dataBlocks[i]) {
            // 如果找到了键
            if (kvp.key == key) {
                // 将值赋值给 value 参数
                value = kvp.value;
                // 返回 true
                return true;
            }
        }
    }
    return false;
}


void BufferIndex::add(BufferBlock* bufferBlockPtr) {
    std::string minKey = bufferBlockPtr->getMinKey();
    std::string maxKey = bufferBlockPtr->getMaxKey();
    int id = bufferBlockPtr->getId();
    int seq = bufferBlockPtr->getSeq();
    Manifest manifest(id, minKey, maxKey, bufferBlockPtr, false, seq);
    indexMap.insert({{minKey, maxKey}, manifest});
}

void BufferIndex::haveWritten(BufferBlock* bufferBlockPtr) {
    std::string minKey = bufferBlockPtr->getMinKey();
    std::string maxKey = bufferBlockPtr->getMaxKey();
    auto it = indexMap.find({minKey, maxKey});
    if (it != indexMap.end()) {
        it->second.isWrittenToSST = true;
        delete it->second.bufferBlockPtr;
    }

}

void BufferIndex::flush() {
    std::cout<<"BufferIndex共索引"<<indexMap.size()<<"个BufferBlock"<<std::endl;
   std::vector<BufferBlock*> blocksToWrite;
    for (auto& [key, manifest] : indexMap) {
        if (!manifest.isWrittenToSST) {
            blocksToWrite.push_back(manifest.bufferBlockPtr);
            manifest.isWrittenToSST = true;
        }
    }
    std::cout<<"正在flush"<<blocksToWrite.size()<<"个BufferBlock"<<std::endl;

    std::vector<std::thread> threads;
    for (BufferBlock* block : blocksToWrite) {
        threads.push_back(std::thread([block] { block->writeToSST(); }));
    }

    for (std::thread& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}

std::string BufferIndex::compaction(){
    std::ostringstream data;
    auto left = this->indexMap.begin();
    auto right = this->indexMap.begin();
    std::advance(right, 1);
    std::cout<<"开始合并"<<std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    // for (const auto& pair : indexMap) {
    // std::cout << pair.first.first << "--" << pair.first.second << "   ";
    // }
    //std::cout << std::endl;
    while (left != indexMap.end()) {
        // Find the range of overlapping Manifests
        std::string maxKey = left->second.maxKey;
        while (right != indexMap.end() && right->first.first <= maxKey) {
            maxKey=max(maxKey,right->first.second);
            ++right;
        }

        if(right==std::next(left)){
            left=right;
            if(right!=indexMap.end()){
                ++right;
            }
            continue;
        }

        // Read data from SST files and remove the corresponding SST files
        std::vector<Manifest> manifests;

        for (auto it = left; it != right;) {
            if (it->second.isWrittenToSST) {
                it->second.bufferBlockPtr=new BufferBlock(it->second.id,it->second.seq);
                it->second.isWrittenToSST=false;
            }
            manifests.push_back(it->second);
            it = indexMap.erase(it);
        }
        std::cout<<"合并"<<manifests.size()<<"个数据块"<<std::endl;

        data<<"合并"<<manifests.size()<<"个数据块"<<std::endl;
        struct KVS{
            std::string key;
            std::string value;
            int seq;
        };
        // Create a min heap
        auto cmp = [](const KVS& a, const KVS& b) {
            return a.key > b.key || (a.key == b.key && a.seq < b.seq);
        };
        std::priority_queue<KVS, std::vector<KVS>, decltype(cmp)> minHeap(cmp);

        // Insert the smallest key of each Manifest into the min heap
        for (const Manifest& manifest : manifests) {
            KeyValuePair kvp;
            while (manifest.bufferBlockPtr->serial_get(kvp)) {
                KVS kvs;
                kvs.key = kvp.key;
                kvs.value = kvp.value;
                kvs.seq = manifest.seq;
                minHeap.push(kvs);
            }
        }
        // Create a new BufferBlock
        BufferBlock* newBlock = new BufferBlock(global_id++,0);

        // Merge the keys
        std::string lastKey;
        while (!minHeap.empty()) {
            KVS kvs= minHeap.top();
            minHeap.pop();

            // Skip the same key
            if (kvs.key == lastKey) {
                continue;
            }

            // Add the key-value pair to the new BufferBlock
            bool flag=newBlock->add(KeyValuePair(kvs.key, kvs.value));
            newBlock->seq=std::max(newBlock->seq,kvs.seq);
            if(flag==false){
                this->add(newBlock);
                newBlock = new BufferBlock(global_id++,0);
                newBlock->add(KeyValuePair(kvs.key, kvs.value));
                newBlock->seq=kvs.seq;
            }

            lastKey = kvs.key;
        }

        // Add the last BufferBlock to the BufferIndex
        this->add(newBlock);

        // Move the left pointer to the right pointer
        left = right;
        if (right != indexMap.end()) {
            ++right;
        }
    }
    std::cout<<"合并结束"<<std::endl;
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    std::cout << "    本次合并时间：" << duration << "s" << std::endl;
    flush();
    return data.str();
}

bool BufferIndex::get(const std::string& key, std::string& value) {
    std::string temp_value;
    int seq=0;
    for (const auto& [keyPair, manifest] : indexMap) {
        if (key >= keyPair.first && key <= keyPair.second) {
            if (!manifest.isWrittenToSST) {
                if (manifest.bufferBlockPtr->get(key, temp_value)) {
                    if(seq<manifest.seq){
                        seq=manifest.seq;
                        value=temp_value;
                    }
                }
            }
            else{
                std::ifstream file("/mnt/intel/zyb/" + std::to_string(manifest.id) + ".sst", std::ios::binary);
            if (!file.is_open()) {
                return false;
            }
            // 移动到文件的最后四个字节
            file.seekg(-4, std::ios::end);

            // 读取索引的偏移量
            int indexOffset;
            file.read((char*)&indexOffset, sizeof(indexOffset));

            // 移动到索引的开始位置
            file.seekg(indexOffset, std::ios::beg);

            // 读取索引
            char index[28];
            int cnt=0;
            while (file.read(index, sizeof(index))) {
                // 如果索引的前 20 个字节大于等于要查找的键
                if (std::string(index, 20) >= key) {
                    // 从索引的中间 4 个字节获取偏移量
                    int offset = *(int*)(index + 20);

                    // 移动到偏移量指向的位置
                    file.seekg(offset, std::ios::beg);

                    // 读取键值对
                    char kvp[40];
                    file.read(kvp, sizeof(kvp));

                    // 如果找到了键
                    if (std::string(kvp, 20) == key) {
                        // 将值赋值给 value 参数
                        temp_value = std::string(kvp + 20, 20);
                        // 关闭文件
                        file.close();
                        // 返回 true
                        if(seq<manifest.seq){
                            seq=manifest.seq;
                            value=temp_value;
                        }
                    }
                }
                cnt++;
                if(cnt>15)
                    break;
            }
            // 关闭文件
            file.close();
            }
        }
    }
    if(seq==0){
        return false;
    }
    return true;

}