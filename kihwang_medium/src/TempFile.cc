
#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>


#include <stdlib.h>
#include <sys/stat.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <assert.h>
#include <string.h>



#include <FastMemcpy.h>
#define memcpy(a, b, c) memcpy_fast(a, b, c)


#include <Config.hpp>

#include <ChunkManager.hpp>


#include <TempFile.hpp>





TempFile::TempFile(const std::string &file_name, size_t file_size) {

	this->file_size = file_size;
	this->file_name = file_name;

	fd = open(file_name.data(), O_RDWR | O_CREAT | O_TRUNC, 0644);
	assert(fd != -1);

	/* Set file size */
	int ret = ftruncate(fd, file_size);
	assert(ret == 0);

	file_map = (char*) mmap(0, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	assert(file_map != NULL);

}


TempFile::~TempFile() {

	close(fd);

}


Chunk* TempFile::read_chunk(int64_t offset, int64_t len) {

	assert(offset + len <= this->file_size);
	assert(len <= CHUNK_SIZE);

	/* Get a chunk */
	Chunk* chunk = chunk_manager->get_chunk();
	char* out_buffer = chunk->buffer;
	assert(out_buffer != NULL);


	/* Do read */
	for (int64_t i = 0; i < len; i += READ_IO_SIZE) {
		int64_t io_size;
		if (i + READ_IO_SIZE <= len) {
			io_size = READ_IO_SIZE;
		} else {
			io_size = len % READ_IO_SIZE;
		}
		//int ret = pread(fd, out_buffer + i, io_size, offset + i);
		//if (ret == -1) std::cout << strerror(errno) << "\n";
		//assert(ret == io_size);
		memcpy(out_buffer + i, file_map + offset + i, io_size);
	}

	chunk->len = len;

	return chunk;
}



void TempFile::write_chunk(Chunk* chunk, int64_t offset) {

	char* in_buffer = chunk->buffer;
	int64_t len = chunk->len;

	assert(offset + len <= file_size);

	/* Do write */
	for (int64_t i = 0; i < len; i += WRITE_IO_SIZE) {
		int64_t io_size;
		if (i + WRITE_IO_SIZE <= len) {
			io_size = WRITE_IO_SIZE;
		} else {
			io_size = len % WRITE_IO_SIZE;
		}
		//int ret = pwrite(fd, in_buffer + i, io_size, offset + i);
		//assert(ret == io_size);
		memcpy(file_map + offset + i, in_buffer + i, io_size);
	}
}













