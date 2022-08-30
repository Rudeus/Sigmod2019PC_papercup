#pragma once





#include <Chunk.hpp>




class PartFile {

	public:
	int64_t file_size;
	std::string file_name;
	int fd;

	char* file_map;




	public:
	PartFile(const std::string &file_name);
	~PartFile();

	Chunk* read_chunk(int64_t offset, int64_t len);
	void write_chunk_next(Chunk* chunk);

};







