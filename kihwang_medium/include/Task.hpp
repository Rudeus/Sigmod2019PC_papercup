#pragma once

#include <TaskManager.hpp>
#include <Chunk.hpp>


enum {
		TASK_SAMPLE,
		TASK_READ_INPUT,
		TASK_QSORT_CHUNK,
		TASK_MERGE_SORT,
		TASK_FLUSH,
		TASK_PREFETCH,
		TASK_SAMPLING,
		TASK_PARTITIONING,
		TASK_PART_IN_MEM,
		TASK_PART,
		TASK_SORT_PART,
		TASK_FLUSH_ALL_PART,
		TASK_FLUSH_ONE_PART,
};

struct task_sample_args {
	int a;
	int b;
};

struct task_read_input_args {
	TaskManager* task_manager;
	int64_t offset;
	int64_t len;
};

struct task_qsort_chunk_args {
	TaskManager* task_manager;
	Chunk* chunk;
};

struct task_merge_sort_args {
	TaskManager* task_manager;
};

struct task_flush_args {
	TaskManager* task_manager;
	Chunk* chunk;
	int64_t offset;
};

struct task_prefetch_args {
	TaskManager* task_manager;
	int64_t offset;
	int64_t len;
};

struct task_sampling_args {
	TaskManager* task_manager;
	int64_t chunk_num;
};

struct task_partitioning_args {
	TaskManager* task_manager;
	int64_t tid;
	int64_t offset;
	int64_t len;
};

struct task_part_in_mem_args {
	TaskManager* task_manager;
	int64_t offset;
	int64_t len;
};

struct task_part_args {
	TaskManager* task_manager;
	Chunk* chunk;
};

struct task_sort_part_args {
	TaskManager* task_manager;
	int64_t tid;
};

struct task_flush_all_part_args {
	TaskManager* task_manager;
};

struct task_flush_one_part_args {
	TaskManager* task_manager;
	int64_t part_index;
};


class Task {

	public:
	int task;
	void* args;


	public:
	Task();
	~Task();

	void make_task(int task, void* arg);
	void run();


};



/* Helper functions to make task */
extern void make_sampling_task(TaskManager* task_manager, int64_t chunk_num);
extern void make_partitioning_task(TaskManager* task_manager, int64_t tid, int64_t offset, int64_t len);
extern void make_read_input_task(TaskManager* task_manager, int64_t offset, int64_t len);
extern void make_qsort_chunk_task(TaskManager* task_manager, Chunk* chunk);
extern void make_merge_sort_task(TaskManager* task_manager);
extern void make_flush_task(TaskManager* task_manager, Chunk* chunk, int64_t offset);
extern void make_prefetch_task(TaskManager* task_manager, int64_t offset, int64_t len);
extern void make_part_in_mem_task(TaskManager* task_manager, int64_t offset, int64_t len);
extern void make_part_task(TaskManager* task_manager, Chunk* chunk);
extern void make_sort_part_task(TaskManager* task_manager, int64_t tid);
extern void make_flush_all_part_task(TaskManager* task_manager);
extern void make_flush_one_part_task(TaskManager* task_manager, int64_t part_index);






