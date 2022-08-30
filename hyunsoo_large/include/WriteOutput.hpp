#pragma once


#include <thread>
#include <mutex>
#include <condition_variable>

#include <Config.hpp>
#include <ChunkManager.hpp>
#include <Chunk.hpp>




class WriteOutput {

	public:

	int64_t file_size;
	std::string outfile;
	int fd;
	char* file_map;

	int* part_fd[READ_THREAD_NUM];
	int64_t output_offset;

	public:
	WriteOutput(const std::string &infile);
	~WriteOutput();


	void write_chunk(Chunk* chunk, int id);
	void end();

};


extern WriteOutput* write_output;


