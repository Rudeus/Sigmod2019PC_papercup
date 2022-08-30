#pragma once


#include <vector>


#include <stdint.h>


#include <Config.hpp>

#include <Chunk.hpp>



/* Sorted chunks list */
class Run {


	public:
	/* Bytes size */
	int64_t len;
	int64_t chunk_num;
	std::vector<Chunk*> chunks;

	bool is_flushed;
	
	/* For read iter */
	int64_t r_offset;

	/* For write iter */
	int64_t w_offset;

	/* For flush iter */
	int64_t ff_offset;


	public:
	Run(Chunk* chunk);
	Run(int64_t chunk_num, int64_t len);
	~Run();

	char* get_ptr(int64_t offset);
	char* read_next();
	void write_next(char* ptr);
	void full_flush_next(char* ptr, int id);
};












