#include <iostream>
#include <thread>
#include <vector>
#include <chrono>

void worker(int id) {
    std::cout << "Worker " << id << " started\n";
    
    // Simulate CPU work
    volatile long long sum = 0;
    for (long long i = 0; i < 1e8; i++) {
        sum += i;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::cout << "Worker " << id << " finished\n";
}

int main() {
    const int NUM_WORKERS = 500000000; // adjust carefully
    std::vector<std::thread> threads;

    for (int i = 0; i < NUM_WORKERS; i++) {
        threads.emplace_back(worker, i);
    }

    for (auto &t : threads) {
        t.join();
    }

    std::cout << "All workers completed\n";
    return 0;
}