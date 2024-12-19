cmake_minimum_required(VERSION 3.5)

project(MyTestModule2)

# 查找 GoogleTest 和 Threads（如果你不使用 GoogleTest，可以去掉这些）
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

# 设定源文件
set(TEST_SRC test_dmclock_burst.cc)

# 创建可执行文件
add_executable(test_dmclock_burst ${TEST_SRC})

# 设置其他头文件目录（例如如果你使用了其他库）
target_include_directories(test_dmclock_burst PRIVATE ../src)

# 链接 dmclock 库
target_link_libraries(test_dmclock_burst
  dmclock
  GTest::GTest
  GTest::Main
  Threads::Threads
)

# 启用测试（如果你使用 GoogleTest）
enable_testing()

# 定义一个单独的测试目标（如果你使用 GoogleTest）
add_test(NAME MyTestModule2Test COMMAND test_dmclock_burst)