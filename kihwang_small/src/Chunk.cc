#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <iomanip>
#include <ctime>
#include <thread>



#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <jemalloc/jemalloc.h>


#include <FastMemcpy.h>
#define memcpy(a, b, c) memcpy_fast(a, b, c)


#include <Config.hpp>



#include <Chunk.hpp>








Chunk::Chunk() {
	//buffer = (char*) malloc(CHUNK_SIZE);
	buffer = (char*) aligned_alloc(512, CHUNK_SIZE);
	assert((int64_t) buffer % 512 == 0);
	len = 0;
	done = false;
	assert(buffer != NULL);
}

Chunk::~Chunk() {
	free(buffer);
}


void Chunk::write_next(char* ptr) {
	assert(len + TUPLESIZE <= CHUNK_SIZE);

	memcpy(buffer + len, ptr, TUPLESIZE);
	len += TUPLESIZE;
}








