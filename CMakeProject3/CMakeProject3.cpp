// CMakeProject3.cpp: 定义应用程序的入口点。
//

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
atomic<int> statue = 1;
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
	virtual void run() = 0;
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
	void run()  override {
		running = true;
		cout << "[Filter-" << key << "开始]" << endl;
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
		cout << "[Filter-" << key << "结束]" << endl;
	}
	void stop() override {
		running = false;
	}
	void callback(int msg)override {
		if (p_in != nullptr) {
			p_in->store(msg, memory_order_relaxed);
		}
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


using namespace std;
class PipelineManager {
private:
	atomic<int> out{ 0 };
	vector< unique_ptr<atomic<int>>>buffers;
	/*为了避免 vector 扩容导致的指针失效，我们需要：
		将 buffers 改为 vector<unique_ptr<atomic<int>>>，即存储指向 atomic<int> 的智能指针；
		这样，vector 扩容时，只需要复制 / 移动智能指针（而不是 atomic<int> 对象），智能指针指向的 atomic<int> 对象的地址不会变化；
		任务的 p_in / p_out 指针指向智能指针管理的 atomic<int> 对象，地址永远不会改变。*/
	vector<unique_ptr<sensortaskbase>>tasks;
	vector<thread>task_threads;
	thread monitor_thread;
	thread command_thread;
	atomic<bool>running{ true };
	mutex mtx;
public:
	PipelineManager() {
		monitor_thread = thread(&PipelineManager::monitor_out, this);
		command_thread = thread(&PipelineManager::process_commands, this);
	}
	~PipelineManager() {
		running = false;
		//for(auto&task:tasks){
		//task->stop();
		//	}
		for (auto& t : task_threads) {
			if (t.joinable()) {
				t.join();
			}
		}
		if (monitor_thread.joinable()) {
			monitor_thread.join();
		}
		if (command_thread.joinable()) {
			command_thread.join();
		}
	}
private:
	void monitor_out() {
		while (running) {
			int val = out.load(memory_order_relaxed);
			if (val != 0) {
				cout << "[PipelineManager] 输出值: " << val << endl;
				out.store(0, memory_order_relaxed);
			}
			this_thread::sleep_for(chrono::milliseconds(10));
		}
	}
	void process_commands() {
		string cmd;
		while (running && cin >> cmd) {
			if (cmd == "add") {
				int key;
				string kind;
				cout << "请输入key和kind（Filter  Gain  Delay)" << endl << endl;
				if (cin >> key >> kind) {
					add_task(key, kind);
				}
			}
			else if (cmd == "pop") {
				pop_task();
			}
			else if (cmd == "exit") {
				statue = 0;
				running = false;
			}
			else if (cmd == "callback") {
				int key, msg;
				cout << "请输入key和msg" << endl << endl;
				if (cin >> key >> msg) {
					callback_task(key, msg);
				}
				else {
					cout << "error:invalid command!" << endl;
					return;
				}
			}
		}
	}

public:
	void add_task(int key, const string& kind) {
		lock_guard<mutex>lock(mtx);
		unique_ptr<sensortaskbase>task;

		if (kind == "Filter") {
			task = make_unique<TaskFilter>(key);
		}
		else if (kind == "Gain") {
			task = make_unique<TaskGain>(key);
		}
		else if (kind == "Delay") {
			task = make_unique < TaskDelayBuffer>(key);
		}

		else {
			cout << "error:invalid task kind!" << endl;
			return;
		}
		buffers.emplace_back(make_unique<atomic<int>>(0));
		//•	错误 C2672 出现在调用 buffers.emplace_back(0);：此调用试图用整型 0 来构造 unique_ptr<atomic<int>>，编译器无法找到匹配的重载（因此报 C2672）。
		//•	修复方法：改为 buffers.emplace_back(make_unique<atomic<int>>(0)); ，显式插入一个 unique_ptr<atomic<int>>，避免类型不匹配。
		size_t n = tasks.size();
		atomic<int>* p_in = buffers.back().get();
		atomic<int>* p_out;
		if (n == 0) {
			p_out = &out;
		}
		else {
			p_out = buffers[n - 1].get();
		}
		task->setpointer(p_in, p_out);
		tasks.push_back(move(task));
		task_threads.emplace_back(&sensortaskbase::run, tasks.back().get());
		cout << "added task with key " << key << " of kind " << ":" << kind << endl;
	}
	void pop_task() {
		lock_guard<mutex>lock(mtx);
		if (tasks.empty()) {
			cout << "error:no tasks to pop!" << endl;
			return;
		}
		tasks.back()->stop();
		if (task_threads.back().joinable()) {
			task_threads.back().join();
		}
		tasks.pop_back();
		task_threads.pop_back();
		buffers.pop_back();
		cout << "popped last task" << endl;
	}
	void callback_task(int key, int msg) {
		lock_guard<mutex>lock(mtx);
		for (auto& task : tasks) {
			if (task->get_key() == key) {
				task->callback(msg);
				return;
			}
		}cout << "error:no task with key " << key << " found!" << endl;
	}
};

int main() {
	PipelineManager manager;
	cout << "PipelineManager 已启动。请输入命令（add/pop/callback）。" << endl << "你也可以输入exit来退出" << endl;
	// 主线程等待用户输入命令
	while (statue != 0) {
		this_thread::sleep_for(chrono::seconds(1));

	}
	return 0;
}
//当你添加任务时，新任务会被插入到流水线的最前端，而不是末尾。这是由 add_task 方法中的指针绑定逻辑决定的：
//key是任务的标识，与流水线的车间所在的顺序无关
