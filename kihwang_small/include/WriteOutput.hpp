#pragma once


#include <thread>
#include <mutex>
#include <condition_variable>

#include <ChunkManager.hpp>
#include <Chunk.hpp>




class WriteOutput {

	public:

	int64_t file_size;
	std::string outfile;
	int fd;

	char* file_map;

	FILE* file_p;

	char buf[WRITE_IO_SIZE] __attribute__((aligned(512)));
	int64_t buf_offset = 0;
	int64_t file_offset = 0;

	public:
	WriteOutput(const std::string &outfile);
	~WriteOutput();


	void write_chunk(Chunk* chunk, int64_t offset);
	void write(char* buffer, int64_t len, int64_t offset);
	void end();

};


extern WriteOutput* write_output;


