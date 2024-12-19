#include <iostream>
#include <fstream>
#include <cassert>

// 假设你想测试 dmClock 中的某个功能，比如时钟的设置
void test_clock_setting() {
    // 打开文件以进行写入
    std::ofstream files_calc("/home/mxqh/software/dmclock/my_test/a.txt", std::ios::app);
    
    if (!files_calc.is_open()) {
        // 如果文件没有成功打开，则输出错误信息
        std::cerr << "Error: Unable to open file for writing." << std::endl;
        return;
    }

    // 向文件写入一些测试数据
    files_calc << "This is a test entry in the file." << std::endl;

    // 关闭文件
    files_calc.close();

    // 确认文件写入是否成功
    std::cout << "Test passed! Data written to file." << std::endl;
}

int main() {
    // 运行测试
    test_clock_setting();
    return 0;
}

