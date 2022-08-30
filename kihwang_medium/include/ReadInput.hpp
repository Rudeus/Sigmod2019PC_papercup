#pragma once


#include <thread>
#include <mutex>
#include <condition_variable>

#include <stdio.h>
#include <stdlib.h>


#include <ChunkManager.hpp>
#include <Chunk.hpp>




class ReadInput {

	public:

	int64_t file_size;
	std::string infile;
	int fd;

	FILE* file_p;

	char* file_map;

	public:
	ReadInput(const std::string &infile);
	~ReadInput();


	Chunk* read_chunk(int64_t offset, int64_t len);

};


extern ReadInput* read_input;


