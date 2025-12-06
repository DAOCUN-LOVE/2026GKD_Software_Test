#include<iostream>
#include <thread>             // std::thread、std::this_thread
#include <mutex>              // std::mutex、std::lock_guard、std::unique_lock、std::once_flag
#include <condition_variable> // std::condition_variable
#include <atomic>             // std::atomic
#include <future>             // std::async、std::future、std::promise、std::packaged_task
#include <functional>         // std::function、std::bind、std::ref
#include <memory>             // std::shared_ptr、std::make_shared、std::unique_ptr
#include <queue>              // std::queue（在线程池、生产者消费者中用）
#include <vector>             // std::vector（线程池里的线程数组）
#include <chrono>             // std::chrono::seconds、milliseconds 等
#include <stdexcept>          // std::runtime_error 等异常类型
using namespace std;
class sensortaskbase {
protected:
	atomic<int> key;
	atomic<int>* p_in;
	atomic<int>* p_out;
	atomic<bool> running;
	mutex mtx;

public:
	sensortaskbase(int task_key) :key(task_key), p_in(nullptr), p_out(nullptr), running(false) {}
	virtual~sensortaskbase() = default;
	virtual void run()=0;
	virtual void stop() = 0;
	virtual void callback(int msg) = 0;
	void setpointer(atomic<int>* in, atomic<int>* out) {
		p_in = in;
		p_out = out;
	}
	int get_key()const {
		return key.load(memory_order_relaxed);
	}
	};

class TaskFilter :public sensortaskbase {
public:
	using sensortaskbase::sensortaskbase;//看看没有override会怎样
	void run()  override{
		running = true;
		cout << "[Filter-" << key << "开始]"<<endl;
		while (running) {
			//检查是否有任务
			if (p_in == nullptr || p_out == nullptr) {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
				continue;
			}
			int input_val = p_in->load(memory_order_relaxed);
			if (input_val != 0) {
				int output_val = input_val + 1;
				p_out->store(output_val, memory_order_relaxed);
				p_in->store(0, memory_order_relaxed);
				cout << "writer filter-" << ":" << output_val << endl;
			}
			std::this_thread::sleep_for(chrono::milliseconds(1));

		}
		cout << "[Filter-" << key << "结束]"<<endl;
	}
	void stop() override {
		running = false;
	}
	void callback(int msg)override {
		if (p_in != nullptr) {
			p_in->store(msg, memory_order_relaxed);}
			cout << "[Filter-" << key << "] 回调注入值: " << msg << endl;  // 调试日志（可选）
		
	}
};
class TaskGain :public sensortaskbase {
private:
	atomic<int> k;
public:
	using sensortaskbase::sensortaskbase;
	TaskGain(int task_key) :sensortaskbase(task_key), k(1) {};
	void run()override {
		running = true;
		cout << "[Gain-" << key << "开始]" << "放大倍数k==" << k << endl;
		while (running) {
			if (p_in == nullptr || p_out == nullptr) {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
				continue;
			}

			int input_val = p_in->load(memory_order_relaxed);
			if (input_val != 0) {
				int output_val = input_val * k;
				p_out->store(output_val, memory_order_relaxed);//int current_k = k.load(memory_order_relaxed);  // 读取当前k值 int output_val = input_val * current_k;
				p_in->store(0, memory_order_relaxed);
				cout << "writer gain-" << ":" << output_val << endl;
			}
			std::this_thread::sleep_for(chrono::milliseconds(1));
		}
		cout << "[Gain-" << key << "结束]" << endl;
	}
	void stop() override {
		running = false;
	}
	void callback(int msg) override {
		k.store(msg, memory_order_relaxed);
		if (p_in != nullptr) {
			p_in->store(1, memory_order_relaxed);
		}
		cout << "[Gain-" << key << "] 回调注入值: " << msg << endl;  // 调试日志（可选）
	}
};
class TaskDelayBuffer :public sensortaskbase {
public:
	using sensortaskbase::sensortaskbase;
	void run()override {
		running = true;
		cout << "[DelayBuffer-" << key << "开始]" << endl;
		while (running) {
			if (p_in == nullptr || p_out == nullptr) {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
				continue;
			}
			int input_val = p_in->load(memory_order_relaxed);
			if (input_val != 0) {
				int t = input_val;
				p_in->store(0, memory_order_relaxed);
				p_out->store(t, memory_order_relaxed);
				cout << "writer delaybuffer-" << ":" << t << endl;
				this_thread::sleep_for(chrono::milliseconds(1));//模拟延时
				p_out->store(t + 1, memory_order_relaxed);
				cout << "writer delaybuffer-" << ":" << t + 1 << endl;
			}
			this_thread::sleep_for(chrono::milliseconds(1));
		}
		cout << "[DelayBuffer-" << key << "] 结束" << endl;
	}
	void stop() override {
		running = false;
	}
	void callback(int msg) override {
		if (p_in != nullptr) {
			p_in->store(msg, memory_order_relaxed);
		}
		cout << "[DelayBuffer-" << key << "] 回调注入值: " << msg << endl;  // 调试日志（可选）
	}
};


int main() {
	//第一个程序：滤波器任务的test
	atomic<int>input_buf = 0;
	atomic<int>output_buf = 0;
	TaskFilter filter_task(1);
	filter_task.setpointer(&input_buf, &output_buf);
	thread task_thread(&TaskFilter::run, &filter_task);
	this_thread::sleep_for(chrono::milliseconds(100));
	filter_task.callback(10);
	this_thread::sleep_for(chrono::milliseconds(100));
	filter_task.stop();
	if (task_thread.joinable()) {// 等待线程退出
		task_thread.join();
	}
	

	cout << "最终输出结果为：" << output_buf.load() << endl;
	cout << filter_task.get_key()<<endl;//查看任务序数

	//第二个程序：增益任务的test
	TaskGain gain_task(2);
	gain_task.setpointer(&input_buf, &output_buf);
	thread gain_thread(&TaskGain::run, &gain_task);
	this_thread::sleep_for(chrono::milliseconds(100));
	gain_task.callback(5); //设置增益k=5
	this_thread::sleep_for(chrono::milliseconds(100));
	gain_task.callback(3); //注入数据3
	this_thread::sleep_for(chrono::milliseconds(100));
	gain_task.stop();
	// 等待线程退出
	if (gain_thread.joinable()) {
		gain_thread.join();
	}


	//第三个程序：延时缓冲任务的test
	TaskDelayBuffer delay_task(3);
	delay_task.setpointer(&input_buf, &output_buf);
	thread delay_thread(&TaskDelayBuffer::run, &delay_task);
	this_thread::sleep_for(chrono::milliseconds(100));
	delay_task.callback(7); //注入数据7
	this_thread::sleep_for(chrono::milliseconds(200));
	delay_task.stop();
	// 等待线程退出
	if (delay_thread.joinable()) {
		delay_thread.join();
	}

	cout << "最终输出结果为：" << output_buf.load() << endl;	
	return 0;
}