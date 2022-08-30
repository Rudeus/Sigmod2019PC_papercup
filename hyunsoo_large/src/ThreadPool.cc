


#include <iostream>
#include <vector>
#include <thread>

#include <Task.hpp>

#include <ThreadPool.hpp>



ThreadPool* thread_pool;







ThreadPool::ThreadPool(int64_t num) {

	this->stop = false;
	this->num = num;

	for (int64_t i = 0; i < num; i++) {
		this->thread_list.emplace_back(std::thread(&ThreadPool::worker_thread_func, this));
	}

}

ThreadPool::~ThreadPool() {

	{
		std::unique_lock<std::mutex> lock(this->task_list_lock);
		this->stop = true;
	}

	this->task_list_condition.notify_all();

	for (int64_t i = 0; i < this->num; i++) {
		this->thread_list[i].join();
	}
}


void ThreadPool::worker_thread_func(void) {

	for (;;) {
		Task* task;
		{
			std::unique_lock<std::mutex> lock(this->task_list_lock);
			this->task_list_condition.wait(lock,
					[this]{ return this->stop || !this->task_list.empty(); });
			if (this->stop && this->task_list.empty()) {
				return;
			}

			/* Get task */
			task = this->task_list.back();
			this->task_list.pop_back();
		}

		/* Do it */
		task->run();
		delete task;
	}

}


int ThreadPool::insert_task(Task* task) {

	{
		std::unique_lock<std::mutex> lock(this->task_list_lock);

		if (this->stop == true) {
			return -1;
		}

		this->task_list.emplace_front(task);
	}

	this->task_list_condition.notify_one();

	return 0;
}



















