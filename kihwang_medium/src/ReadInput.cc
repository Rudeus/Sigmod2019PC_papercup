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

/* From https://stackoverflow.com/questions/23306552/getting-a-files-size-in-c-with-lseek */
size_t fileSize(const char* filename)
{
//	size_t rv = 0;  // I like to return 0, if badness occurs
//	struct stat  file_info;
//
//	if ( (filename != NULL) && (stat(filename,&file_info) == 0) )  //NULL check/stat() call
//		rv = (size_t)file_info.st_size;  // Note: this may not fit in a size_t variable
//
//	return rv;
	int fd = open(filename, O_RDONLY);
	size_t size = lseek(fd, 0, SEEK_END);
	close(fd);
	return size;
}


ReadInput::ReadInput(const std::string &infile) {
//	struct stat st;
//	stat(infile.data(), &st);
//#ifdef VERBOSE
//	std::cout << "file size : " << st.st_size << "\n";
//#endif
//	this->file_size = st.st_size;
	this->file_size = (int64_t) fileSize(infile.data());
	this->infile = infile;

	/* TODO: O_DIRECT */
	fd = open(infile.data(), O_RDONLY | O_DIRECT);
	assert(fd != -1);

//	file_p = fopen(infile.data(), "rb");
//	assert(file_p != NULL);

//	file_map = (char*) mmap(0, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
//	assert(file_map != NULL);

	/* TODO: fadvise */
	//posix_fadvise(fd, 0, file_size, POSIX_FADV_SEQUENTIAL | POSIX_FADV_NOREUSE);
}

ReadInput::~ReadInput() {
	close(fd);
}



/* Read and return a chunk */
Chunk* ReadInput::read_chunk(int64_t offset, int64_t len) {

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
		int ret = pread(fd, out_buffer + i, io_size, offset + i);
		assert(ret == io_size);
		//memcpy(out_buffer + i, file_map + offset + i, io_size);
//		int ret = fread(out_buffer, 1, io_size, file_p);
//		assert(ret == io_size);
	}


	chunk->len = len;

	return chunk;
}











