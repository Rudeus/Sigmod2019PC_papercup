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

	fd = open(outfile.data(), O_RDWR | O_CREAT | O_TRUNC, 0644);
	assert(fd != -1);

	/* Set file size */
	int ret = ftruncate(fd, file_size);
	assert(ret == 0);

	//file_map = (char*) mmap(0, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	//assert(file_map != MAP_FAILED);


	if (ext_flag[part_id]) {
		for (int i = 0; i < READ_THREAD_NUM; i++) {
			part_fd[i] = (int*)malloc(sizeof(int) * PART_NUM - inmemory_part_num);
		}
		//part_#threadid_0

		std::string path = outfile;
		std::size_t point = path.find_last_of("/");
		std::string dir = path.substr(0, point);
		std::string file = path.substr(point + 1, path.length());
		
		char str[100];
		for (int i = 0; i < READ_THREAD_NUM; i++) {
			for (int j = 0; j < PART_NUM - inmemory_part_num; j++) {
				sprintf(str, "part_%d%d", i, j);
				std::string filename0 = path + std::string(str);
				part_fd[i][j] = open(filename0.data(), O_RDWR|O_CREAT, 0666);
				assert(part_fd[i][j] != -1);
			}
		}

		output_offset = 0;
	}
}

WriteOutput::~WriteOutput() {
	if (ext_flag[part_id]) {
		for (int i = 0; i < READ_THREAD_NUM; i++) {
			for (int j = 0; j < PART_NUM - inmemory_part_num; j++) {
				close(part_fd[i][j]);
			}
			free(part_fd[i]);
		}
	}
}




void WriteOutput::write_chunk(Chunk* chunk, int id) {


#ifdef VERBOSE
	std::cout << "write chunk : " << chunk->real_loc << "\n";
#endif


	int64_t offset = chunk->real_loc * CHUNK_SIZE;
	assert(offset < file_size);

	int64_t len = chunk->len;

	char* in_buffer = chunk->buffer;
	int64_t out_buffer = offset;
	//if (ext_flag[id]) {
	for (int i = 0; i < id; i++)
		out_buffer += part_size[i];
	//}

	/*if (ext_flag[id] == 0) {
		// Rollback endian
		for (int64_t i = 0; i < len; i += TUPLESIZE) {
			uint64_t* ptr = (uint64_t*) (in_buffer + i);
			*ptr = __builtin_bswap64(*ptr);
			uint16_t* ptr2 = (uint16_t*) (in_buffer + i + 8);
			*ptr2 = __builtin_bswap16(*ptr2);
		}
	}*/
	pwrite(fd, in_buffer, len, out_buffer);

	//memcpy(out_buffer, in_buffer, len);

//	int ret = msync(out_buffer, size, MS_SYNC);
//	assert(ret == 0);

}


void WriteOutput::end() {
	//munmap(file_map, file_size);
	close(fd);
}










