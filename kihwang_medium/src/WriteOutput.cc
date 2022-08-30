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

#include <ReadInput.hpp>
#include <ChunkManager.hpp>
#include <Chunk.hpp>




#include <WriteOutput.hpp>


WriteOutput* write_output;



WriteOutput::WriteOutput(const std::string &outfile) {
	file_size = read_input->file_size;
	this->outfile = outfile;

	fd = open(outfile.data(), O_RDWR | O_CREAT | O_TRUNC, 0777);
	assert(fd != -1);

	/* Set file size */
	int ret = ftruncate(fd, file_size);
	assert(ret == 0);

//	file_p = fopen(outfile.data(), "wb");
//	assert(file_p != NULL);

	file_map = (char*) mmap(0, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	assert(file_map != NULL);
	/* TODO: fadvise */
	//posix_fadvise(fd, 0, file_size, POSIX_FADV_SEQUENTIAL | POSIX_FADV_NOREUSE);

	/* TODO: madvise */
	//madvise(file_map, file_size, MADV_SEQUENTIAL | MADV_DONTFORK | MADV_HUGEPAGE | MADV_DONTDUMP | MADV_WILLNEED);
}

WriteOutput::~WriteOutput() {
	//munmap(file_map, file_size);
	close(fd);
}




void WriteOutput::write_chunk(Chunk* chunk, int64_t offset) {

	char* in_buffer = chunk->buffer;
	int64_t len = chunk->len;

	assert(offset + len <= file_size);

//	/* Rollback endian */
//	for (int64_t i = 0; i < len; i += TUPLESIZE) {
//		uint64_t* ptr = (uint64_t*) (in_buffer + i);
//		*ptr = __builtin_bswap64(*ptr);
//		uint16_t* ptr2 = (uint16_t*) (in_buffer + i + 8);
//		*ptr2 = __builtin_bswap16(*ptr2);
//	}

	/* Do write */
	for (int64_t i = 0; i < len; i += WRITE_IO_SIZE) {
		int64_t io_size;
		if (i + WRITE_IO_SIZE <= len) {
			io_size = WRITE_IO_SIZE;
		} else {
			io_size = len % WRITE_IO_SIZE;
		}
		int ret = pwrite(fd, in_buffer + i, io_size, offset + i);
		assert(ret == io_size);
		//memcpy(file_map + offset + i, in_buffer + i, io_size);
	}
//	int ret = msync(out_buffer, len, MS_SYNC);
//	assert(ret == 0);

}

void WriteOutput::write(char* buffer, int64_t len, int64_t offset) {


	assert(offset + len <= file_size);
	int ret = pwrite(fd, buffer, len, offset);
	assert(ret == len);

//	if (buf_offset + len <= WRITE_IO_SIZE) {
//		memcpy(buf + buf_offset, buffer, len);
//		buf_offset += len;
//	} else {
//		memcpy(buf + buf_offset, buffer, WRITE_IO_SIZE - buf_offset);
//		int ret = pwrite(fd, buf, WRITE_IO_SIZE, file_offset);
//		assert(ret == WRITE_IO_SIZE);
//		file_offset += WRITE_IO_SIZE;
//
//		memcpy(buf, buffer + WRITE_IO_SIZE - buf_offset, len - WRITE_IO_SIZE + buf_offset);
//		buf_offset = len - WRITE_IO_SIZE + buf_offset;
//	}

//		int ret = fwrite(buffer, 1, len, file_p);
//		assert(ret == len);
}

void WriteOutput::end() {
//	int64_t ret = pwrite(fd, buf, WRITE_IO_SIZE, file_offset);
//	assert(ret == WRITE_IO_SIZE);
//
//	ret = ftruncate(fd, file_size);
//	assert(ret == 0);
}










