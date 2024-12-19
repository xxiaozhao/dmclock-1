#include <iostream>
#include <chrono>
#include <memory>
#include <vector>
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
    crimson::dmclock::ReqParams req_params;
    uint64_t ops_count = 0;
    uint64_t outstanding_ops = 0;
    uint64_t max_outstanding_ops;
    uint64_t iops_goal;
    bool is_burst;

public:
    TestClient(uint64_t _iops_goal, uint64_t _max_outstanding_ops, bool _is_burst = false) :
        req_params(1.0, 1.0),
        max_outstanding_ops(_max_outstanding_ops),
        iops_goal(_iops_goal),
        is_burst(_is_burst) {}

    bool can_send_request() {
        return outstanding_ops < max_outstanding_ops;
    }

    TestRequest create_request() {
        return TestRequest(++ops_count, is_burst);
    }

    void request_complete() {
        --outstanding_ops;
    }

    bool is_burst_client() const {
        return is_burst;
    }
};

// 服务器类
class TestServer {
    uint64_t iops_capacity;
    std::chrono::microseconds service_time;

public:
    TestServer(uint64_t _iops_capacity) :
        iops_capacity(_iops_capacity),
        service_time(std::chrono::microseconds(1000000 / _iops_capacity)) {}

    void process_request(const TestRequest& req, std::function<void(TestResponse)> cb) {
        std::this_thread::sleep_for(service_time);
        cb(TestResponse(req));
    }
};

int main() {
    // 创建服务器
    auto server = std::make_shared<TestServer>(1000); // 1000 IOPS capacity

    // 创建普通客户端
    std::vector<std::shared_ptr<TestClient>> normal_clients;
    for (int i = 0; i < 100; ++i) {
        normal_clients.push_back(std::make_shared<TestClient>(100, 10));
    }

    // 创建突发客户端
    auto burst_client = std::make_shared<TestClient>(1000, 50, true);

    // 创建dmclock队列
    crimson::dmclock::ClientInfo normal_info(5.0, 1.0, 100.0);
    crimson::dmclock::ClientInfo burst_info(0.0, 1.0, 500.0, 500, 500);

    auto client_info_f = [&](const int& client_id) -> const crimson::dmclock::ClientInfo* {
        if (client_id == 100) { // burst client id
            return &burst_info;
        }
        return &normal_info;
    };

    crimson::dmclock::PullPriorityQueue<int, TestRequest> queue(client_info_f);

    // 模拟运行
    const auto start_time = std::chrono::steady_clock::now();
    const auto run_duration = std::chrono::seconds(60);

    while (std::chrono::steady_clock::now() - start_time < run_duration) {
        // 处理普通客户端请求
        for (int i = 0; i < normal_clients.size(); ++i) {
            if (normal_clients[i]->can_send_request()) {
                auto req = normal_clients[i]->create_request();
                queue.add_request(std::move(req), i, crimson::dmclock::ReqParams(1.0, 1.0));
            }
        }

        // 处理突发客户端请求
        if (burst_client->can_send_request()) {
            auto req = burst_client->create_request();
            queue.add_request(std::move(req), 100, crimson::dmclock::ReqParams(1.0, 1.0), 1, true);
        }

        // 处理请求
        auto req = queue.pull_request();
        if (req.is_retn()) {
            auto& retn = req.get_retn();
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

                    // 打印请求信息
                    std::cout << "Time: " << elapsed << "s | "
                             << "Client: " << (retn.client == 100 ? "BURST" : std::to_string(retn.client)) 
                             << " | Request ID: " << resp.req.id
                             << " | Phase: " << (retn.phase == crimson::dmclock::PhaseType::reservation ? "RESV" :
                                               retn.phase == crimson::dmclock::PhaseType::priority ? "PROP" : "UNKNOWN")
                             << " | Processing Time: " << duration << "us"
                             << std::endl;

                    // 完成请求处理
                    if (retn.client == 100) {
                        burst_client->request_complete();
                    } else {
                        normal_clients[retn.client]->request_complete();
                    }
                }
            );
        }
    }

    return 0;
}  