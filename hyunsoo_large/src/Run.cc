//develop


#include <cstdlib>
#include <cstdint>
#include <iostream>


#include <assert.h>
#include <string.h>




#include <ChunkManager.hpp>
#include <ThreadPool.hpp>
#include <Task.hpp>


#include <Run.hpp>





Run::Run(Chunk* chunk) {
	chunk_num = 1;
	chunks.emplace_back(chunk);
	len = chunk->len;

	is_flushed = false;
	r_offset = 0;
	w_offset = 0;
	ff_offset = 0;
}

Run::Run(int64_t chunk_num, int64_t len) {
	this->chunk_num = chunk_num;
	this->len = len;
	chunks.resize(chunk_num, NULL);

	is_flushed = false;
	r_offset = 0;
	w_offset = 0;
	ff_offset = 0;
}

Run::~Run() {

}

char* Run::get_ptr(int64_t offset) {

	assert(offset < len);

	Chunk* chunk = chunks[offset / CHUNK_SIZE];

	char* ret = chunk->buffer + (offset % CHUNK_SIZE);

	return ret;
}


char* Run::read_next() {

	if (r_offset > len) {
		return NULL;
	}

	if (r_offset == len) {
		Chunk* chunk = chunks[(r_offset - TUPLESIZE) / CHUNK_SIZE];
		chunks[(r_offset - TUPLESIZE) / CHUNK_SIZE] = NULL;
		chunk_manager->free_chunk(chunk);
		r_offset += TUPLESIZE;
		return NULL;
	}

	if (r_offset % CHUNK_SIZE == 0) {
		if (r_offset != 0) {
			/* This chunk don't need anymore */
			Chunk* chunk = chunks[(r_offset - TUPLESIZE) / CHUNK_SIZE];
			chunks[(r_offset - TUPLESIZE) / CHUNK_SIZE] = NULL;
			chunk_manager->free_chunk(chunk);
		}
	}

	Chunk* chunk = chunks[r_offset / CHUNK_SIZE];

	assert(chunk != NULL);
	assert(r_offset % CHUNK_SIZE < chunk->len);
	char* ret = chunk->buffer + (r_offset % CHUNK_SIZE);
	r_offset += TUPLESIZE;

	return ret;
}

void Run::write_next(char* ptr) {

	if (w_offset >= len) {
		return;
	}

	Chunk* chunk = chunks[w_offset / CHUNK_SIZE];
	if (chunk == NULL) {
		chunk = chunk_manager->get_free_chunk();
		chunks[w_offset / CHUNK_SIZE] = chunk;
		chunk->len = 0;
	}

	assert(chunk->len <= CHUNK_SIZE);

	memcpy(chunk->buffer + chunk->len, ptr, TUPLESIZE);

	chunk->len += TUPLESIZE;
	w_offset += TUPLESIZE;
}



void Run::full_flush_next(char* ptr, int id) {

	if (ff_offset >= len) {
		return;
	}

	Chunk* chunk = chunks[ff_offset / CHUNK_SIZE];
	if (chunk == NULL) {
		chunk = chunk_manager->get_free_chunk();
		chunks[ff_offset / CHUNK_SIZE] = chunk;
		chunk->len = 0;
		chunk->real_loc = ff_offset / CHUNK_SIZE;
	}
	assert(chunk->len <= CHUNK_SIZE);
	/*if (ff_offset % (1000LL * 1000 * 1000 * 2) == 0) {
		print_time("flush 2G");
	}*/

	memcpy(chunk->buffer + chunk->len, ptr, TUPLESIZE);

	chunk->len += TUPLESIZE;
	ff_offset += TUPLESIZE;

	if (chunk->len == CHUNK_SIZE || ff_offset == len) {
		Task* task;
		task = new Task();
		task_flush_real_args* args = (task_flush_real_args*) malloc(sizeof(task_flush_real_args));
		args->chunk = chunk;
		args->id = id;
		task->make_task(TASK_FLUSH_REAL, args);
		thread_pool->insert_task(task);
		chunks[(ff_offset - TUPLESIZE) / CHUNK_SIZE] = NULL;
	}
}





