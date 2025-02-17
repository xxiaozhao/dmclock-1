#include <iostream>
#include <chrono>
#include <memory>
#include <vector>
#include <fstream>
#include <cstdlib> // for std::atoi
#include "dmclock_server.h"

// 定义请求类型
struct TestRequest {
    uint64_t id;
    bool is_burst;
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    
    TestRequest(uint64_t _id, bool _is_burst = false) : 
        id(_id),
        is_burst(_is_burst),
        start_time(std::chrono::steady_clock::now()) {}
};

// 定义响应类型
struct TestResponse {
    TestRequest req;
    std::chrono::time_point<std::chrono::steady_clock> end_time;
    
    TestResponse(const TestRequest& _req) :
        req(_req),
        end_time(std::chrono::steady_clock::now()) {}
};

// 客户端类
class TestClient {
public:
    crimson::dmclock::ReqParams req_params;
    uint64_t ops_count = 0;
    uint64_t outstanding_ops = 0;
    uint64_t max_outstanding_ops;
    uint64_t iops_goal;
    bool is_burst;
    bool is_first_add;
    std::chrono::time_point<std::chrono::steady_clock> next_request_time;
    uint64_t next_request_count;
    

public:
    TestClient(uint64_t _iops_goal, uint64_t _max_outstanding_ops, bool _is_burst = false) :
        req_params(1.0, 1.0),
        max_outstanding_ops(_max_outstanding_ops),
        iops_goal(_iops_goal),
        is_burst(_is_burst),
        is_first_add(true),
        next_request_time(std::chrono::steady_clock::now()){} // 初始化下一个请求时间为当前时间加1秒

    bool can_send_request() {
        if(is_burst){
            if (is_first_add && outstanding_ops < max_outstanding_ops && std::chrono::steady_clock::now() >= next_request_time) {
                return true;
            }else
                return false;

        }else
            return outstanding_ops < max_outstanding_ops;

        
    }

    TestRequest create_request() {

        outstanding_ops++;
        return TestRequest(++ops_count, is_burst);

    }

    void request_complete() {
        --outstanding_ops;
    }

    bool is_burst_client() const {
        return is_burst;
    }

    uint64_t get_outstanding_ops() const {
        return outstanding_ops;
    }
};

// 服务器类
class TestServer {
    uint64_t iops_capacity;
    std::chrono::microseconds service_time;

public:
    TestServer(uint64_t _iops_capacity) :
        iops_capacity(_iops_capacity),
        // service_time(std::chrono::microseconds(_iops_capacity < 500000?(1000000 / _iops_capacity):0)) {
        service_time(std::chrono::microseconds(1000000 / _iops_capacity)) {
            std::cout << "service_time: " << service_time.count() << "us" << std::endl;
        }

    void process_request(const TestRequest& req, std::function<void(TestResponse)> cb) {
        try {

            auto start = std::chrono::high_resolution_clock::now();
            auto end = start + service_time;

            while (std::chrono::high_resolution_clock::now() < end) {
                // 使用pause指令来减少功耗（仅在x86架构上有效）
                asm volatile("pause" ::: "memory");
            }

            cb(TestResponse(req));
        } catch (const std::exception& e) {
            std::ofstream log_file("error_log.txt", std::ios::app);
            if (log_file.is_open()) {
                log_file << "Exception caught: 处理休眠" << e.what() << std::endl;
                log_file.close();
            }
        } catch (...) {
            std::ofstream log_file("error_log.txt", std::ios::app);
            if (log_file.is_open()) {
                log_file << "Unknown exception caught:处理休眠" << std::endl;
                log_file.close();
            }
        }
    }
};

int main(int argc, char* argv[]) {
    // 检查命令行参数
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <number_of_normal_clients> <number_of_burst_clients>" << std::endl;
        return 1;
    }

    // 解析命令行参数
    int num_normal_clients = std::atoi(argv[1]);
    int num_burst_clients = std::atoi(argv[2]);

// int main() {

//     // 解析命令行参数
//     int num_normal_clients = 1;
//     int num_burst_clients = 0;

    // 创建服务器
    auto server = std::make_shared<TestServer>(5000000); // 500000000 IOPS capacity

    // 创建普通客户端
    std::vector<std::shared_ptr<TestClient>> normal_clients;
    for (int i = 0; i < num_normal_clients; ++i) {
        normal_clients.push_back(std::make_shared<TestClient>(100, 100));
    }

    // 创建突发客户端
    std::vector<std::shared_ptr<TestClient>> burst_clients;
    for (int i = 0; i < num_burst_clients; ++i) {
        burst_clients.push_back(std::make_shared<TestClient>(100, 100, true));
    }

    // 创建dmclock队列
    crimson::dmclock::ClientInfo normal_info(0.0, 1.0, 0.0);
    crimson::dmclock::ClientInfo burst_info(0.0, 100.0, 1000.0, 100, 1000);
    crimson::dmclock::ClientInfo burst_info_high(0.0, 1.0, 1000.0, 100, 1000);

    auto client_info_f = [&](const int& client_id) -> const crimson::dmclock::ClientInfo* {
        if (client_id >= num_normal_clients) { // burst client id
            int burst_client_id = client_id - num_normal_clients;

            if(burst_client_id%2 == 0)
                return &burst_info;
            else
                return &burst_info_high;
        }
        return &normal_info;
    };

    crimson::dmclock::PullPriorityQueue<int, TestRequest, true, true, 2> queue(client_info_f);

    // 计数器
    int reservation_count = 0;
    int burst_count = 0;
    int priority_count = 0;
    std::vector<int> every_burst_count(num_burst_clients,0);




            // 处理普通客户端请求
        for (int i = 0; i < normal_clients.size(); ++i) {
            while (normal_clients[i]->can_send_request()) {
                auto req = normal_clients[i]->create_request();
                queue.add_request(std::move(req), i, crimson::dmclock::ReqParams(0, 0));
            }
        }



        // 处理突发客户端请求
        for (int i = 0; i < burst_clients.size(); ++i) {
             int requests_to_add = 1000; // 每0.1秒添加500个请求
                while (requests_to_add > 0 && burst_clients[i]->can_send_request()) {
                    auto req = burst_clients[i]->create_request();
                    queue.add_request(std::move(req), num_normal_clients + i, crimson::dmclock::ReqParams(0, 0), 1, true);
                    requests_to_add--;
                }

                // std::cout<<"首次添加完成："<<burst_clients[i]->outstanding_ops<<std::endl;
            burst_clients[i]->next_request_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(1000); // 生成新的随机时间间隔，范围为500到1000毫秒
        }

    // 模拟运行
    const auto start_time = std::chrono::steady_clock::now();
    const auto run_duration = std::chrono::seconds(10);
    

    while (std::chrono::steady_clock::now() - start_time < run_duration) {


        // // 处理突发客户端请求
        // static auto last_add_time = std::chrono::steady_clock::now();
        // static auto last_period_time = std::chrono::steady_clock::now();
        // auto current_time = std::chrono::steady_clock::now();
        // auto time_since_last_add = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - last_add_time).count();

        // if (time_since_last_add >= 100) { // 时间间隔调整为0.1秒
        //     for (int i = 0; i < burst_clients.size(); ++i) {
        //         int requests_to_add = 1000; // 每0.1秒添加50个请求
        //         bool is_added = false;
        //         while (requests_to_add > 0 && burst_clients[i]->can_send_request()) {
        //             is_added = true;
        //             auto req = burst_clients[i]->create_request();
        //             queue.add_request(std::move(req), num_normal_clients + i, crimson::dmclock::ReqParams(0, 0), 1, true);
        //             requests_to_add--;
        //         }
        //         if(is_added)
        //             burst_clients[i]->is_first_add = false;
        //         // std::cout<<"当前队列请求数量："<<burst_clients[i]->outstanding_ops<<std::endl;
        //     }


        //     last_add_time = current_time;
        // }


        // //  每个周期都自动修改下次添加请求时间
        // auto time_since_last_period = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - last_period_time).count();
        // if(time_since_last_period >= 500)  //每500毫秒生成随机生成下次添加请求的时间
        // {
        //     for (int i = 0; i < burst_clients.size(); ++i) {
        //         burst_clients[i]->next_request_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(rand() % 400);
        //         burst_clients[i]->is_first_add = true;
        //     }
        //     last_period_time = current_time;
        // }





        // 处理请求
        auto req = queue.pull_request();
        if (req.is_retn()) {
            auto& retn = req.get_retn();
            if (!retn.request) {
                std::cout << "Invalid request pointer***********************************************" << std::endl;
                continue;
            }
            
            // 根据阶段更新计数器
            if (retn.phase == crimson::dmclock::PhaseType::reservation) {
                ++reservation_count;
            } else if (retn.phase == crimson::dmclock::PhaseType::burst) {
                ++burst_count;
            } else if (retn.phase == crimson::dmclock::PhaseType::priority) {
                ++priority_count;
            }

            server->process_request(
                *retn.request,
                [&](const TestResponse& resp) {
                    // 计算请求处理时间
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                        resp.end_time - resp.req.start_time).count();
                    
                    // 获取当前时间点
                    auto current_time = std::chrono::steady_clock::now();
                    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                        current_time - start_time).count();

                    // // 打印突发请求处理情况
                    // if(retn.phase == crimson::dmclock::PhaseType::burst)
                    //         std::cout << "Time: " << elapsed << "s | "
                    //          << "Client: " << (std::to_string(retn.client)) 
                    //          << " | Request ID: " << resp.req.id
                    //          << " | Phase: " << (retn.phase == crimson::dmclock::PhaseType::reservation ? "RESV" :
                    //                            retn.phase == crimson::dmclock::PhaseType::priority ? "PROP" : "BURST")
                    //          << " | Processing Time: " << duration << "us"
                    //          << std::endl;


                    // 完成请求处理
                    if (retn.client >= num_normal_clients) {
                        burst_clients[retn.client - num_normal_clients]->request_complete();

                        //添加请求的数据
                        int burst_id = retn.client - num_normal_clients;
                        every_burst_count[burst_id]++;
                        auto req = burst_clients[retn.client - num_normal_clients]->create_request();
                        queue.add_request(std::move(req), retn.client, crimson::dmclock::ReqParams(0, 0), 1, true);

                    } else {
                        normal_clients[retn.client]->request_complete();
                        auto req = normal_clients[retn.client]->create_request();
                        queue.add_request(std::move(req), retn.client, crimson::dmclock::ReqParams(0, 0));
                    }

                }
            );
        }
    }


//   // 打印每个突发客户端处理的请求数量
//     for(int i=0;i<every_burst_count.size();i++){
//         std::cout <<"客户端"<< i+1<<":"<< every_burst_count[i]<<"\t"<<std::endl;
//     }

    // 打印统计信息
    std::cout << "\n=== Final Statistics ===\n"
              << "Reservation Phase Requests: " << reservation_count << "\n"
              << "Burst Phase Requests: " << burst_count << "\n"
              << "Priority Phase Requests: " << priority_count << "\n"
              << "Total Requests: " << (reservation_count + burst_count + priority_count) << "\n"
              << "=====================\n" << std::endl;

    std::cout << "\n恭喜，程序运行完毕！" << std::endl;

    return 0;
}  