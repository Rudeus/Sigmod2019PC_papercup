#pragma once





#include <Chunk.hpp>




class TempFile {

	public:
	int64_t file_size;
	std::string file_name;
	int fd;

	char* file_map;




	public:
	TempFile(const std::string &file_name, size_t file_size);
	~TempFile();

	Chunk* read_chunk(int64_t offset, int64_t len);
	void write_chunk(Chunk* chunk, int64_t offset);

};







