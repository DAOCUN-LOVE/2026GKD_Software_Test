#include <iostream>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include <chrono>
#include <string>
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
		for (auto& t : task_threads) {
			if (t.joinable) {
				t.join();
			}
		}
		if (monitor__thread.joinable()) {
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
				cout << "请输入key和kind" << endl << endl;
				if (cin >> key >> kind) {
					add_task(key, kind);
				}
			}
			else if (cmd == "pop") {
				pop_task();
			}
			else if (cmd = "callback") {
				int key, msg;
				cout << "请输入key和msg" << endl << endl;
				if (cin >> key >> msg) {
					callback_task(key, msg);
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
			task = make_unique < TaskDelayBuffer>(keay);
		}
		else {
			cout << "erroe:invalid task kind!" << endl;
			return;
		}
		buffers.emplace_back(0);
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
	}
	void pop_task{
		lock_guard<mutex>lock(mtx);
	if (tasks, empty()) {
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
	};
		