#pragma once



#include <list>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>

#include <Task.hpp>




/** 
 * This threadpool class is based on 
 * https://github.com/progschj/ThreadPool/blob/master/ThreadPool.h
 **/
class ThreadPool {

	public:
	int64_t num;
	std::vector<std::thread> thread_list;

	/* Task list */
	std::list<Task*> task_list;

	/* Synchronization */
	std::mutex threadpool_lock;
	std::condition_variable threadpool_condition;
	bool stop;


	public:
	ThreadPool(int64_t num);
	~ThreadPool();

	void worker_thread_func();
	int insert_task(Task* task);


};



extern ThreadPool* thread_pool;









