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


#include <Config.hpp>

#include <ChunkManager.hpp>
#include <Chunk.hpp>




#include <ReadInput.hpp>


ReadInput* read_input;



ReadInput::ReadInput(const std::string &infile) {
	struct stat st;
	stat(infile.data(), &st);
#ifdef VERBOSE
	std::cout << "file size : " << st.st_size << "\n";
#endif
	this->file_size = st.st_size;
	this->infile = infile;

	fd = open(infile.data(), O_RDONLY);
	assert(fd != -1);


	//posix_fadvise(fd, 0, file_size, POSIX_FADV_SEQUENTIAL);

	// for prohibit run large size
	//if (file_size > CHUNK_SIZE * CHUNK_NUM)
	//	exit(0);
}

ReadInput::~ReadInput() {
	close(fd);
}




Chunk* ReadInput::read_chunk(int64_t offset, int64_t len) {

//#ifdef VERBOSE
//	std::cout << "read chunk : " << chunk_id << "\n";
//#endif
//
//	/* Get a free chunk */
//	Chunk* chunk = chunk_manager->get_free_chunk();
//	char* out_buffer = chunk->buffer;
//	assert(out_buffer != NULL);
//
//
//	int64_t offset = chunk_id * CHUNK_SIZE;
//	if (offset >= this->file_size)
//		return NULL;
//	assert(offset < this->file_size);
//
//	int64_t len = CHUNK_SIZE;
//	if (offset + CHUNK_SIZE > this->file_size) {
//		len = this->file_size % CHUNK_SIZE;
//		assert(offset + len == this->file_size);
//	}
//
//	int ret = pread(fd, out_buffer, len, offset);
//	assert(ret == len);
//
//	/* Change endian */
//	for (int64_t i = 0; i < len; i += TUPLESIZE) {
//		uint64_t* ptr = (uint64_t*) (out_buffer + i);
//		*ptr = __builtin_bswap64(*ptr);
//		uint16_t* ptr2 = (uint16_t*) (out_buffer + i + 8);
//		*ptr2 = __builtin_bswap16(*ptr2);
//	}
//
//	chunk->len = len;
//	chunk->real_loc = chunk_id;
//	chunk->temp_loc = chunk_id;
//
//
//	return chunk;



	assert(offset + len <= this->file_size);
	assert(len <= CHUNK_SIZE);

	/* Get a chunk */
	Chunk* chunk = chunk_manager->get_free_chunk();
	char* out_buffer = chunk->buffer;
	assert(out_buffer != NULL);


	/* Do read */
	for (int64_t i = 0; i < len; i += CHUNK_SIZE) {
		int64_t io_size;
		if (i + CHUNK_SIZE <= len) {
			io_size = CHUNK_SIZE;
		} else {
			io_size = len % CHUNK_SIZE;
		}
		int ret = pread(fd, out_buffer + i, io_size, offset + i);
		assert(ret == io_size);
	}

	/* Change endian */
	for (int64_t i = 0; i < len; i += TUPLESIZE) {
		uint64_t* ptr = (uint64_t*) (out_buffer + i);
		*ptr = __builtin_bswap64(*ptr);
		uint16_t* ptr2 = (uint16_t*) (out_buffer + i + 8);
		*ptr2 = __builtin_bswap16(*ptr2);
	}

	chunk->len = len;

	return chunk;

}











