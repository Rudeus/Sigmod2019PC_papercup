#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>




#include <stdlib.h>
#include <assert.h>
#include <jemalloc/jemalloc.h>



#include <Config.hpp>



#include <Chunk.hpp>








Chunk::Chunk() {
	buffer = (char*) malloc(CHUNK_SIZE);
	assert(buffer != NULL);
}

Chunk::Chunk(int64_t size) {
	buffer = (char*)malloc(size);
	assert(buffer != NULL);
}

Chunk::~Chunk() {
	free(buffer);
}










