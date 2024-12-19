cmake_minimum_required(VERSION 3.5)

project(MyTestModule)

# 查找 GoogleTest 和 Threads（如果你不使用 GoogleTest，可以去掉这些）
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

# 设定源文件
set(TEST_SRC my_test_module.cpp)

# 创建可执行文件
add_executable(my_test_module ${TEST_SRC})

# 设置其他头文件目录（例如如果你使用了其他库）
# target_include_directories(my_test_module PRIVATE ../src)

# 不需要链接 dmclock 库，因为你目前不使用它
# target_link_libraries(my_test_module
#   GTest::GTest
#   GTest::Main
#   Threads::Threads
# )

# 启用测试（如果你使用 GoogleTest）
# enable_testing()

# 定义一个单独的测试目标（如果你使用 GoogleTest）
# add_test(NAME MyTestModuleTest COMMAND my_test_module)

