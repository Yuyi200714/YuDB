cmake_minimum_required(VERSION 3.10) # 指定 CMake 的最低版本要求

project(YuDB) # 定义项目名称

set(CMAKE_CXX_STANDARD 17) # 设置 C++ 标准
set(CMAKE_BUILD_TYPE Debug) # 启用调试信息


# Masstree
aux_source_directory(${PROJECT_SOURCE_DIR}/lib/masstree MASSTREE_SOURCE_FILES)
add_library(masstree ${MASSTREE_SOURCE_FILES})
target_compile_options(masstree PRIVATE
          $<$<COMPILE_LANGUAGE:CXX>:-include ${PROJECT_SOURCE_DIR}/lib/masstree/config.h>)
 
# 添加头文件目录
target_include_directories(masstree PUBLIC ${PROJECT_SOURCE_DIR}/lib/masstree)
target_include_directories(masstree PUBLIC ${PROJECT_SOURCE_DIR}/include)

# 定义一个可执行文件
add_executable(YuDB src/BST.cpp src/KVDB.cpp src/main.cpp src/Buffer.cpp)

# 为 KVdb 目标添加包含目录
target_include_directories(YuDB PRIVATE ${PROJECT_SOURCE_DIR}/lib/masstree)
target_include_directories(YuDB PRIVATE ${PROJECT_SOURCE_DIR}/include)

 
# 将静态库 MyLib 链接到可执行文件 MyApp 上
target_link_libraries(YuDB PRIVATE masstree)
