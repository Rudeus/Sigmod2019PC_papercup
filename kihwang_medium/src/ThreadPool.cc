


#include <iostream>
#include <vector>
#include <thread>

#include <Task.hpp>

#include <ThreadPool.hpp>



ThreadPool* thread_pool;







ThreadPool::ThreadPool(int64_t num) {

	stop = false;
	this->num = num;

	for (int64_t i = 0; i < num; i++) {
		thread_list.emplace_back(std::thread(&ThreadPool::worker_thread_func, this));
	}
	
}

ThreadPool::~ThreadPool() {

	{
		std::unique_lock<std::mutex> lock(threadpool_lock);
		stop = true;
	}

	threadpool_condition.notify_all();

	for (int64_t i = 0; i < num; i++) {
		thread_list[i].join();
	}
}


void ThreadPool::worker_thread_func(void) {

	for (;;) {
		Task* task;
		{
			std::unique_lock<std::mutex> lock(threadpool_lock);


			threadpool_condition.wait(lock,
					[this]{ return this->stop ||
					!this->task_list.empty(); 
					});

			if (stop && task_list.empty()) {
				return;
			}

			/* Get task */
			/* TODO: task scheduling */
			if (task_list.empty() == false) {
				task = task_list.back();
				task_list.pop_back();
			} else {
				continue;
			}
		}

		/* Do it */
		task->run();
		delete task;
	}

}


int ThreadPool::insert_task(Task* task) {

	{
		std::unique_lock<std::mutex> lock(threadpool_lock);

		if (stop == true) {
			return -1;
		}

		task_list.emplace_front(task);
	}

	threadpool_condition.notify_one();

	return 0;
}




