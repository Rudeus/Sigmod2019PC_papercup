#pragma once


#include <vector>


#include <stdint.h>


#include <Config.hpp>


#include <TaskManager.hpp>
#include <Chunk.hpp>


class TaskManager;


/* Sorted chunks list */
class Run {


	public:
	/* Bytes size */
	int64_t len = 0;
	int64_t chunk_num = 0;
	std::vector<Chunk*> chunks;

	TaskManager* task_manager;

	bool is_flushed = false;

	/* For append iter */
	int64_t a_offset = 0;
	
	/* For read iter */
	int64_t r_offset = 0;

	/* For write iter */
	int64_t w_offset = 0;

	/* For full flush iter */
	int64_t ff_offset = 0;

	/* For part flush iter */
	int64_t pf_offset = 0;
	int64_t flush_point = 0;

	public:
	Run(void);
	Run(Chunk* chunk);
	Run(int64_t chunk_num, int64_t len);
	~Run();

	char* get_ptr(int64_t offset);
	void append(char* ptr);
	char* read_next();
	void write_next(char* ptr);
	void flush_next(char* ptr);
	void full_flush_next(char* ptr);
	void part_flush_next(char* ptr);
};












