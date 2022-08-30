#pragma once


#include <thread>
#include <mutex>
#include <condition_variable>

#include <Config.hpp>
#include <ChunkManager.hpp>
#include <Chunk.hpp>




class ReadInput {

	public:

	int64_t file_size;
	std::string infile;
	int fd;

	public:
	ReadInput(const std::string &infile);
	~ReadInput();


	Chunk* read_chunk(int64_t offset, int64_t len);

};


extern ReadInput* read_input;


