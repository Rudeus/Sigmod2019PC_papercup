


#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <ctime>


#include <string.h>
#include <unistd.h>
#include <assert.h>

#include <FastMemcpy.h>
#define memcpy(a, b, c) memcpy_fast(a, b, c)



#include <ThreadPool.hpp>
#include <ChunkManager.hpp>
#include <Task.hpp>


#include <Run.hpp>




Run::Run() {
	chunks.resize(1, NULL); /* 2G?? */
	for (int64_t i = 0; i < 1; i++) {
		chunks[i] = chunk_manager->get_chunk();
		chunks[i]->len = 0;
	}
}

Run::~Run() {

}


Run::Run(Chunk* chunk) {
	chunk_num = 1;
	len = chunk->len;
	chunks.emplace_back(chunk);
}

Run::Run(int64_t chunk_num, int64_t len) {
	this->chunk_num = chunk_num;
	this->len = len;
	chunks.resize(chunk_num, NULL);
}


char* Run::get_ptr(int64_t offset) {

	assert(offset < len);

	Chunk* chunk = chunks[offset / CHUNK_SIZE];

	char* ret = chunk->buffer + (offset % CHUNK_SIZE);

	return ret;
}

void Run::append(char* ptr) {

	int64_t offset;

	offset = __sync_fetch_and_add(&a_offset, TUPLESIZE);

	Chunk* chunk = chunks[offset / CHUNK_SIZE];
	int64_t chunk_offset = offset % CHUNK_SIZE;
	assert(offset / CHUNK_SIZE < 1);

	memcpy(chunk->buffer + chunk_offset, ptr, TUPLESIZE);

	__sync_fetch_and_add(&chunk->len, TUPLESIZE);
	__sync_fetch_and_add(&len, TUPLESIZE);
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

		if (r_offset + flush_point < len && is_flushed == true
				&& chunks[(r_offset + flush_point) / CHUNK_SIZE] == NULL) {
			/* Prefetch */
			Task* task;
			task = new Task();
			task_prefetch_args* args = (task_prefetch_args*) malloc(sizeof(task_prefetch_args));
			args->task_manager = task_manager;
			args->offset = r_offset;
			//args->index = (r_offset + flush_point) / CHUNK_SIZE;
			task->make_task(TASK_PREFETCH, args);
			thread_pool->insert_task(task);
		}
	}

	Chunk* chunk = chunks[r_offset / CHUNK_SIZE];

	if (chunk == NULL && is_flushed == true) {
		/* TODO: wait until this chunk is read from the temp file*/
		for (;;) {
			chunk = chunks[r_offset / CHUNK_SIZE];
			if (chunk != NULL) {
				break;
			}
			usleep(1000);
		}
	}
	assert(chunk != NULL);

//	if (chunk->done == false) {
//		/* TODO: wait until this chunk is filled by merge sort */
//		for (;;) {
//			chunk = chunks[r_offset / CHUNK_SIZE];
//			if (chunk->done == true) {
//				break;
//			}
//			usleep(1000);
//		}
//
//	}

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
		chunk = chunk_manager->get_chunk();
		chunks[w_offset / CHUNK_SIZE] = chunk;
		chunk->len = 0;
	}
	assert(chunk->len <= CHUNK_SIZE);

	memcpy(chunk->buffer + chunk->len, ptr, TUPLESIZE);

	chunk->len += TUPLESIZE;
	w_offset += TUPLESIZE;
}



void Run::full_flush_next(char* ptr) {

	if (ff_offset >= len) {
		return;
	}

	Chunk* chunk = chunks[ff_offset / CHUNK_SIZE];
	if (chunk == NULL) {
		chunk = chunk_manager->get_chunk();
		chunks[ff_offset / CHUNK_SIZE] = chunk;
		chunk->len = 0;
	}
	assert(chunk->len <= CHUNK_SIZE);
	if (ff_offset % (1000LL * 1000 * 1000 * 2) == 0) {
		print_time("flush 2G");
	}

	memcpy(chunk->buffer + chunk->len, ptr, TUPLESIZE);

	chunk->len += TUPLESIZE;
	ff_offset += TUPLESIZE;

	if (chunk->len == CHUNK_SIZE || ff_offset == len) {
		Task* task;
		task = new Task();
		task_flush_args* args = (task_flush_args*) malloc(sizeof(task_flush_args));
		args->task_manager = task_manager;
		args->chunk = chunk;
		args->offset = ff_offset - chunk->len;
		task->make_task(TASK_FLUSH, args);
		thread_pool->insert_task(task);
		chunks[(ff_offset - TUPLESIZE) / CHUNK_SIZE] = NULL;
	}
}



void Run::part_flush_next(char* ptr) {

	if (pf_offset >= len) {
		return;
	}

	if (pf_offset < flush_point) {
		// in memory

		Chunk* chunk = chunks[pf_offset / CHUNK_SIZE];
		if (chunk == NULL) {
			chunk = chunk_manager->get_chunk();
			chunks[pf_offset / CHUNK_SIZE] = chunk;
			chunk->len = 0;
		}
		assert(chunk->len <= CHUNK_SIZE);

		memcpy(chunk->buffer + chunk->len, ptr, TUPLESIZE);

		chunk->len += TUPLESIZE;
		pf_offset += TUPLESIZE;


	} else { /* pf_offset >= flush_point */
		// flush

		Chunk* chunk = chunks[pf_offset / CHUNK_SIZE];
		if (chunk == NULL) {
			chunk = chunk_manager->get_chunk();
			chunks[pf_offset / CHUNK_SIZE] = chunk;
			chunk->len = 0;
		}
		assert(chunk->len <= CHUNK_SIZE);

		memcpy(chunk->buffer + chunk->len, ptr, TUPLESIZE);

		chunk->len += TUPLESIZE;
		pf_offset += TUPLESIZE;

		if (chunk->len == CHUNK_SIZE || pf_offset == len) {
			Task* task;
			task = new Task();
			task_flush_args* args = (task_flush_args*) malloc(sizeof(task_flush_args));
			args->task_manager = task_manager;
			args->chunk = chunk;
			args->offset = pf_offset - chunk->len - flush_point;
			task->make_task(TASK_FLUSH, args);
			thread_pool->insert_task(task);
			chunks[(pf_offset - TUPLESIZE) / CHUNK_SIZE] = NULL;
		}
	}
}




