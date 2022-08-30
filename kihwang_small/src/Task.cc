#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>


#include <math.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>


#include <Config.hpp>

#include <FastMemcpy.h>
#define memcpy(a, b, c) memcpy_fast(a, b, c);

#include <ThreadPool.hpp>
#include <ReadInput.hpp>
#include <WriteOutput.hpp>
#include <TempFile.hpp>
#include <Run.hpp>
#include <PartFile.hpp>



#include <Task.hpp>


//----------------------------------------------------------------
//----------------------------------------------------------------

/* prefetch */
//void sample_task(int a, int b) {
//	print_time("Prefetch thread on");
//	Chunk* chunk = chunk_manager->get_chunk();
//	chunk->len = CHUNK_SIZE;
//	for (int64_t i = 0; i + chunk->len < write_output->file_size; i += chunk->len) {
//		write_output->write(chunk->buffer, chunk->len, i);
//	}
//	print_time("Prefetch thread off");
//}

void sample_task(int a, int b) {
	print_time("Free thread on");

	Chunk* chunk;

	for (;;) {
		if (chunk_manager->free_chunks.size() == 0) {
			/* Empty */
			break;
		}
		chunk = chunk_manager->free_chunks.front();
		chunk_manager->free_chunks.pop_front();

		free(chunk->buffer);
	}

	print_time("Free thread off");
}
//----------------------------------------------------------------
//----------------------------------------------------------------

void sampling_task(TaskManager* task_manager, int64_t chunk_num) {
	std::cout << "sampling task\n";
	
	Chunk* chunk;
	int64_t offset;
	for (int64_t i = 0; i < chunk_num; i++) {
		/* Read some chunks randomly */
		offset = rand() % (read_input->file_size - CHUNK_SIZE);
		chunk = read_input->read_chunk(offset, CHUNK_SIZE);

		for (int64_t j = 0; j < chunk->len; j += TUPLESIZE) {
			/* Endian */
			uint64_t* loc_ptr = (uint64_t*) (chunk->buffer + j);
			uint64_t loc = __builtin_bswap64(*loc_ptr);
			loc = loc >> (64 - task_manager->prefix_size);

			/* Add on the histogram */
			__sync_fetch_and_add(&task_manager->histogram[loc], 1);
		}

		/* For length of sampling */
		__sync_fetch_and_add(&task_manager->sampling_len, chunk->len);
		chunk_manager->free_chunk(chunk);
	}

	__sync_fetch_and_add(&task_manager->sampling_counter, 1);
}

void make_sampling_task(TaskManager* task_manager, int64_t chunk_num) {
	Task* task = new Task();
	task_sampling_args* args = (task_sampling_args*) malloc(sizeof(task_sampling_args));
	args->task_manager = task_manager;
	args->chunk_num = chunk_num;
	task->make_task(TASK_SAMPLING, args);
	thread_pool->insert_task(task);
}

//----------------------------------------------------------------
//----------------------------------------------------------------

void partitioning_task(TaskManager* task_manager, int64_t tid, int64_t offset, int64_t len) {

	/* Init partition files */
	for (int64_t i = 0; i < task_manager->partition_num; i++) {
		std::string path = write_output->outfile;
		std::string filename = path + "_" + std::to_string(tid) + "_" + std::to_string(i);
		task_manager->part_files[tid][i] = new PartFile(filename);
	}

	/* Buffers for write to partition files */
	std::vector<Chunk*> part_chunk(task_manager->partition_num, NULL);
	for (int64_t i = 0; i < task_manager->partition_num; i++) {
		part_chunk[i] = chunk_manager->get_chunk();
	}

	for (int64_t i = 0; i < len; i += CHUNK_SIZE) {
		int64_t chunk_len = CHUNK_SIZE;
		if (offset + i + CHUNK_SIZE > read_input->file_size) {
			chunk_len = (task_manager->output_run_len - offset) % CHUNK_SIZE;
		}

		/* Read */
		Chunk* read_chunk = read_input->read_chunk(offset + i, chunk_len);
		Chunk* chunk;

		/* Partitioning */
		for (int64_t j = 0; j < read_chunk->len; j += TUPLESIZE) {
			/* Endian */
			uint64_t* loc_ptr = (uint64_t*) (read_chunk->buffer + j);
			uint64_t loc = __builtin_bswap64(*loc_ptr);
			loc = loc >> (64 - task_manager->prefix_size);

			int64_t partition_id = task_manager->partition_map[loc];
			chunk = part_chunk[partition_id];
			chunk->write_next(read_chunk->buffer + j);
			if (chunk->len >= CHUNK_SIZE) {
				/* This chunk is full. */
				__sync_fetch_and_add(&task_manager->real_part_size[partition_id], chunk->len);
				if (partition_id < task_manager->overlap) {
					/* Merge it (in memory) */
					make_qsort_chunk_task(task_manager, chunk);
					part_chunk[partition_id] = chunk_manager->get_chunk();
				} else {
					/* Flush it */
					task_manager->part_files[tid][partition_id]->write_chunk_next(chunk);
					chunk->len = 0;
				}
			}
		}
		chunk_manager->free_chunk(read_chunk);
	}

	/* Flush remained part and free chunks */
	for (int64_t partition_id = 0; partition_id < task_manager->partition_num; partition_id++) {
		Chunk* chunk = part_chunk[partition_id];
		if (chunk->len != 0) {
			__sync_fetch_and_add(&task_manager->real_part_size[partition_id], chunk->len);
			if (partition_id < task_manager->overlap) {
				/* Merge it (in memory) */
				make_qsort_chunk_task(task_manager, chunk);
			} else {
				/* Flush it */
				task_manager->part_files[tid][partition_id]->write_chunk_next(chunk);
				chunk_manager->free_chunk(chunk);
			}
		} else {
			/* Length is 0. Nothing to do. Just free it */
			chunk_manager->free_chunk(part_chunk[partition_id]);
		}
	}

	print_time("partitioning task done");
	__sync_fetch_and_add(&task_manager->partition_counter, len);
}

void make_partitioning_task(TaskManager* task_manager, int64_t tid, int64_t offset, int64_t len) {
	Task* task = new Task();
	task_partitioning_args* args = (task_partitioning_args*) malloc(sizeof(task_partitioning_args));
	args->task_manager = task_manager;
	args->tid = tid;
	args->offset = offset;
	args->len = len;
	task->make_task(TASK_PARTITIONING, args);
	thread_pool->insert_task(task);
}

//----------------------------------------------------------------
//----------------------------------------------------------------

void part_in_mem_task(TaskManager* task_manager, int64_t offset, int64_t len) {

	assert(offset + len <= read_input->file_size);

//	for (int64_t i = 0; i < len; i += CHUNK_SIZE) {
//		int64_t chunk_len = CHUNK_SIZE;
//		if (offset + i + CHUNK_SIZE > read_input->file_size) {
//			chunk_len = (task_manager->output_run_len - offset) % CHUNK_SIZE;
//		} else if (i + CHUNK_SIZE > len) {
//			chunk_len = len - i;
//		}
//
//		/* Read */
//		Chunk* chunk = read_input->read_chunk(offset + i, chunk_len);
//
//		/* Partitioning task */
//		make_part_task(task_manager, chunk);
//	}
	Chunk* chunk = chunk_manager->get_chunk();
	assert((uint64_t) chunk->buffer % 512 == 0);

	for (int64_t i = 0; i < len; i += CHUNK_SIZE) {
		int64_t ret = pread(read_input->fd, chunk->buffer, CHUNK_SIZE, offset + i);
		assert(ret == CHUNK_SIZE || ret == len % CHUNK_SIZE);
		for (int64_t j = 0; j < CHUNK_SIZE; j += TUPLESIZE) {
			if (offset + i + j >= read_input->file_size) break;
			int64_t index = key2pid(chunk->buffer + j);
			int64_t part_offset = __sync_fetch_and_add(&task_manager->partition_lens[index], TUPLESIZE);
			//assert(part_offset < task_manager->partition_size);
			memcpy(task_manager->partition_buffers[index] + part_offset, chunk->buffer + j, TUPLESIZE);
		}
	}

	free(chunk->buffer);

	int64_t counter = __sync_add_and_fetch(&task_manager->part_in_mem_counter, len);
	//int64_t counter = 0;
	if (counter == task_manager->output_run_len) {
		print_time("partition is done");
		mb();
		task_manager->done_condition.notify_one();
	}
}
void make_part_in_mem_task(TaskManager* task_manager, int64_t offset, int64_t len) {
	Task* task = new Task();
	task_part_in_mem_args* args = (task_part_in_mem_args*) malloc(sizeof(task_part_in_mem_args));
	args->task_manager = task_manager;
	args->offset = offset;
	args->len = len;
	task->make_task(TASK_PART_IN_MEM, args);
	thread_pool->insert_task(task);
}

void part_task(TaskManager* task_manager, Chunk* chunk) {
	
	for (int64_t i = 0; i < chunk->len; i += TUPLESIZE) {
//		uint64_t index = __builtin_bswap64(*((uint64_t*) (chunk->buffer + i)));
//		index = index >> (64 - task_manager->part_bit_num);
		int64_t index = key2pid(chunk->buffer + i);
		int64_t part_offset = __sync_fetch_and_add(&task_manager->partition_lens[index], TUPLESIZE);
		//assert(part_offset < task_manager->partition_size);
		memcpy(task_manager->partition_buffers[index] + part_offset, chunk->buffer + i, TUPLESIZE);
	}

	//free(chunk->buffer);
	int64_t counter = __sync_add_and_fetch(&task_manager->part_in_mem_counter, chunk->len);
	if (counter == task_manager->output_run_len) {
		print_time("partition is done");
		task_manager->done = true;
		mb();
		task_manager->done_condition.notify_one();
	}

	chunk_manager->free_chunk(chunk);

}
void make_part_task(TaskManager* task_manager, Chunk* chunk) {
	Task* task = new Task();
	task_part_args* args = (task_part_args*) malloc(sizeof(task_part_args));
	args->task_manager = task_manager;
	args->chunk = chunk;
	task->make_task(TASK_PART, args);
	thread_pool->insert_task(task);
}



//----------------------------------------------------------------
//----------------------------------------------------------------

void read_input_task(TaskManager* task_manager, int64_t offset, int64_t len) {
	
	assert(offset + len <= read_input->file_size);

	for (int64_t i = 0; i < len; i += CHUNK_SIZE) {
		int64_t chunk_len = CHUNK_SIZE;
		if (offset + i + CHUNK_SIZE > read_input->file_size) {
			chunk_len = (task_manager->output_run_len - offset) % CHUNK_SIZE;
		}

		/* Read */
		Chunk* chunk = read_input->read_chunk(offset + i, chunk_len);

		/* Insert a quick sort task */
		make_qsort_chunk_task(task_manager, chunk);
	}

	int64_t counter = __sync_add_and_fetch(&task_manager->read_counter, len);
	if (counter == task_manager->output_run_len) {
		//print_time("local read is done");
	}
}

void make_read_input_task(TaskManager* task_manager, int64_t offset, int64_t len) {
	Task* task = new Task();
	task_read_input_args* args = (task_read_input_args*) malloc(sizeof(task_read_input_args));
	args->task_manager = task_manager;
	args->offset = offset;
	args->len = len;
	task->make_task(TASK_READ_INPUT, args);
	thread_pool->insert_task(task);
}

//----------------------------------------------------------------
//----------------------------------------------------------------

void count_sort(Chunk* chunk1, Chunk* chunk2, int64_t exp) {
	Chunk* chunk;
	Chunk* output_chunk;

	if (chunk1->len != 0) {
		chunk = chunk1;
		output_chunk = chunk2;
	} else {
		chunk = chunk2;
		output_chunk = chunk1;
	}

	std::vector<int64_t> count(256, 0);

	for (int64_t i = 0; i < chunk->len; i += TUPLESIZE) {
		uint8_t index = *((uint8_t*) (chunk->buffer + i + exp));
		count[index]++;
	}

	for (int64_t i = 1; i < 256; i++) {
		count[i] += count[i - 1];
	}

	for (int64_t i = chunk->len - TUPLESIZE; i >= 0; i -= TUPLESIZE) {
		uint8_t index = *((uint8_t*) (chunk->buffer + i + exp));
		memcpy(output_chunk->buffer + ((count[index] - 1) * TUPLESIZE), chunk->buffer + i, TUPLESIZE);
		count[index]--;
	}

	output_chunk->len = chunk->len;
	chunk->len = 0;
}


Chunk* radix_sort(Chunk* chunk) {
	if (chunk->len == 0)
		return chunk;

	Chunk* temp_chunk = chunk_manager->get_chunk();
	temp_chunk->len = 0;
	for (int64_t i = 0; i < KEYSIZE; i++) {
		count_sort(chunk, temp_chunk, KEYSIZE - i - 1);
	}

	if (chunk->len != 0) {
		chunk_manager->free_chunk(temp_chunk);
		return chunk;
	}
	else {
		chunk_manager->free_chunk(chunk);
		return temp_chunk;
	}
}

void count_sort_char(char* buffer1, int64_t* len1, char* buffer2, int64_t* len2, int64_t exp) {
	char* buffer;
	char* output_buffer;
	int64_t* len;
	int64_t* output_len;

	if (*len1 != 0) {
		buffer = buffer1;
		output_buffer = buffer2;
		len = len1;
		output_len = len2;
	} else {
		buffer = buffer2;
		output_buffer = buffer1;
		len = len2;
		output_len = len1;
	}

	std::vector<int64_t> count(256, 0);

	for (int64_t i = 0; i < *len; i += TUPLESIZE) {
		uint8_t index = *((uint8_t*) (buffer + i + exp));
		count[index]++;
	}

	for (int64_t i = 1; i < 256; i++) {
		count[i] += count[i - 1];
	}

	for (int64_t i = *len - TUPLESIZE; i >= 0; i -= TUPLESIZE) {
		uint8_t index = *((uint8_t*) (buffer + i + exp));
		memcpy(output_buffer + ((count[index] - 1) * TUPLESIZE), buffer + i, TUPLESIZE);
		count[index]--;
	}

	*output_len = *len;
	*len = 0;
}

char* radix_sort_char(char* buffer, int64_t len) {
	if (len == 0)
		return buffer;

	char* temp = (char*) malloc(len);
	int64_t temp_len = 0;
	for (int64_t i = 0; i < KEYSIZE; i++) {
		count_sort_char(buffer, &len, temp, &temp_len, KEYSIZE - i - 1);
	}

	if (len != 0) {
		free(temp);
		return buffer;
	}
	else {
		free(buffer);
		return temp;
	}
}

inline int compare(const void* a, const void* b) {
	uint8_t* a_ptr;
	uint8_t* b_ptr;
	for (size_t i = 1; i < KEYSIZE; i += 1) {
		a_ptr = (uint8_t*) ((char*) a + i);
		b_ptr = (uint8_t*) ((char*) b + i);

		if (*a_ptr < *b_ptr) return -1;
		else if (*a_ptr > *b_ptr) return 1;
	}
	return 0;
}

int compare_memcmp(const void* a, const void* b) {

	return memcmp(a, b, KEYSIZE);
}

/* You must check endian */
int compare_64(const void* a, const void* b) {
	if (*((uint64_t*) a) > *((uint64_t*) b)) return 1;
	else if (*((uint64_t*) a) < *((uint64_t*) b)) return -1;

	uint16_t* a_ptr_16 = (uint16_t*) ((char*) a + 8);
	uint16_t* b_ptr_16 = (uint16_t*) ((char*) b + 8);

	if (*a_ptr_16 > *b_ptr_16) return 1;
	else if (*a_ptr_16 < *b_ptr_16) return -1;

	return 0;
}


/* Quick sort it */
void qsort_chunk_task(TaskManager* task_manager, Chunk* chunk) {

	assert(chunk != NULL);

	chunk = radix_sort(chunk);

	char* buffer = chunk->buffer;
	int64_t len = chunk->len;

	/* Change endian */
	for (int64_t i = 0; i < len; i += TUPLESIZE) {
		uint64_t* ptr = (uint64_t*) (buffer + i);
		*ptr = __builtin_bswap64(*ptr);
		uint16_t* ptr2 = (uint16_t*) (buffer + i + 8);
		*ptr2 = __builtin_bswap16(*ptr2);
	}

	/* Do quick sort */
	//qsort(buffer, len / TUPLESIZE, TUPLESIZE, compare_64);

	/* Make run */
	Run* run = new Run(chunk);
	run->task_manager = task_manager;
	run->flush_point = task_manager->flush_point;
	assert(run->chunks.size() != 0);

	/* This run is ready to merged */
	task_manager->insert_run(run);


	/* Insert merge sort task */
	make_merge_sort_task(task_manager);
}

void make_qsort_chunk_task(TaskManager* task_manager, Chunk* chunk) {
	Task* task = new Task();
	task_qsort_chunk_args* args = (task_qsort_chunk_args*) malloc(sizeof(task_qsort_chunk_args));
	args->task_manager = task_manager;
	args->chunk = chunk;
	task->make_task(TASK_QSORT_CHUNK, args);
	thread_pool->insert_task(task);
}


void sort_part_task(TaskManager* task_manager, int64_t tid) {

	int64_t part_index;
	int64_t part_num = task_manager->part_num;

//	for (;;) {
//		part_index = __sync_fetch_and_add(&task_manager->lsn, 1);
//
//		if (part_index >= part_num) {
//			break;
//		}
//
//		char* buffer = task_manager->partition_buffers[part_index];
//		int64_t len = task_manager->partition_lens[part_index];
//
//		if (len == 0) {
//			continue;
//		}
//
//		//buffer = radix_sort_char(buffer, len);
//		qsort(buffer, len / TUPLESIZE, TUPLESIZE, compare);
//
//		mb();
//
//		make_flush_one_part_task(task_manager, part_index);
//	}

//	for (;;) {
//		/* Get global time stamp */
//		task_manager->local_part_index[tid] = task_manager->lsn;
//		mb();
//
//		part_index = __sync_fetch_and_add(&task_manager->lsn, 1);
//
//		if (part_index >= part_num) {
//			task_manager->local_part_index[tid] = -1;
//			if (part_index == part_num + task_manager->sort_thread_num - 1) {
//				print_time("sort done");
//			}
//			break;
//		}
//
//		char* buffer = task_manager->partition_buffers[part_index];
//		int64_t len = task_manager->partition_lens[part_index];
//
//		if (len == 0) {
//			task_manager->local_part_index[tid] = -1;
//			continue;
//		}
//
//		//buffer = radix_sort_char(buffer, len);
//		qsort(buffer, len / TUPLESIZE, TUPLESIZE, compare);
//
//		mb();
//		task_manager->local_part_index[tid] = -1;
//	}

	for (;;) {
		/* Get global time stamp */

		part_index = __sync_fetch_and_add(&task_manager->lsn, 1);

		if (part_index >= part_num) {
			if (part_index == part_num + task_manager->sort_thread_num - 1) {
				print_time("sort done");
			}
			break;
		}

		char* buffer = task_manager->partition_buffers[part_index];
		int64_t len = task_manager->partition_lens[part_index];
		int64_t offset = task_manager->part_offset[part_index];

//		int64_t len = task_manager->partition_lens[part_index];
//		int64_t offset = task_manager->part_offset[part_index];
//		char* buffer = write_output->file_map + offset;

		if (len == 0) {
			continue;
		}

		//buffer = radix_sort_char(buffer, len);
		qsort(buffer, len / TUPLESIZE, TUPLESIZE, compare);

		memcpy(write_output->file_map + offset, buffer, len);
		
		//free(buffer);

		int64_t counter = __sync_add_and_fetch(&task_manager->part_flush_counter, len);
		if (counter == read_input->file_size) {
//			int ret = ftruncate(write_output->fd, read_input->file_size);
//			assert(ret == 0);
			print_time("end");
			exit(0);
		}
	}
}

void make_sort_part_task(TaskManager* task_manager, int64_t tid) {
	Task* task = new Task();
	task_sort_part_args* args = (task_sort_part_args*) malloc(sizeof(task_sort_part_args));
	args->task_manager = task_manager;
	args->tid = tid;
	task->make_task(TASK_SORT_PART, args);
	thread_pool->insert_task(task);
}

/* Write all partitions to the disk */
void flush_all_part_task(TaskManager* task_manager) {
	print_time("Write thread on");

	int64_t part_num = task_manager->part_num;
	int64_t sdl = 0;

	for (;;) {
		int64_t sbl = task_manager->lsn;

		for(int64_t tid = 0; tid < task_manager->sort_thread_num; tid++) {
			int64_t local_sbl = task_manager->local_part_index[tid];
			if (local_sbl != -1) {
				if (local_sbl < sbl) {
					sbl = local_sbl;
				}
			}
		}

		if (sdl < sbl) {
			//printf("%lu\n", sbl - sdl);
			for (int64_t part_index = sdl; part_index < sbl; part_index++) {
				if (part_index >= part_num) {
					write_output->end();
					print_time("end");
					exit(0);
				}
				char* buffer = task_manager->partition_buffers[part_index];
				int64_t len = task_manager->partition_lens[part_index];
				int64_t offset = task_manager->part_offset[part_index];
				if (len == 0) continue;
				write_output->write(buffer, len, offset);
				free(buffer);
			}
			sdl = sbl;
		} else {
			print_time("touch");
			usleep(10000);
		}

		if (sdl >= part_num) {
			write_output->end();
			print_time("end");
			exit(0);
		}
	}

	print_time("end");
	exit(0);

}

void make_flush_all_part_task(TaskManager* task_manager) {
	Task* task = new Task();
	task_flush_all_part_args* args = (task_flush_all_part_args*) malloc(sizeof(task_flush_all_part_args));
	args->task_manager = task_manager;
	task->make_task(TASK_FLUSH_ALL_PART, args);
	thread_pool->insert_task(task);
}


void flush_one_part_task(TaskManager* task_manager, int64_t part_index) {


	char* buffer = task_manager->partition_buffers[part_index];
	int64_t len = task_manager->partition_lens[part_index];
	int64_t offset = task_manager->part_offset[part_index];
	if (len == 0) return;
	write_output->write(buffer, len, offset);
	free(buffer);


	int64_t counter = __sync_add_and_fetch(&task_manager->part_flush_counter, len);
	if (counter == read_input->file_size) {
		print_time("end");
		exit(0);
	}

}

void make_flush_one_part_task(TaskManager* task_manager, int64_t part_index) {
	Task* task = new Task();
	task_flush_one_part_args* args = (task_flush_one_part_args*) malloc(sizeof(task_flush_one_part_args));
	args->task_manager = task_manager;
	args->part_index = part_index;
	task->make_task(TASK_FLUSH_ONE_PART, args);
	thread_pool->insert_task(task);
}

//----------------------------------------------------------------
//----------------------------------------------------------------



struct pair {
	uint64_t num;
	char* ptr;
};

struct compare_pair {
	bool operator() (struct pair a, struct pair b) {
		return compare_64(a.ptr, b.ptr) >= 0;
	}
};

void merge_sort_task(TaskManager* task_manager) {

	/* Get run vector */
	std::vector<Run*>* run_vector = task_manager->get_run_vector();
	if (run_vector == NULL) {
		return;
	}

	/* Make heap */
	std::vector<pair> heap;
	int64_t len = 0;
	int64_t chunk_num = 0;
	for (uint64_t i = 0; i < run_vector->size(); i++) {
		struct pair pair;
		pair.num = i;
		pair.ptr = run_vector->at(i)->read_next();
		heap.push_back(pair);
		
		assert(pair.ptr != NULL);
		len += run_vector->at(i)->len;
		chunk_num += run_vector->at(i)->chunk_num;
	}
	std::make_heap(heap.begin(), heap.end(), compare_pair());

	/* Output run */
	Run* output_run = new Run(chunk_num, len);
	output_run->task_manager = task_manager;
	output_run->flush_point = task_manager->flush_point;

	bool local_last_merge = false;
	bool global_last_merge = false;
	if (output_run->len == task_manager->output_run_len) {
		/* Last merge, write output file */
		local_last_merge = true;
		output_run->is_flushed = true;
		task_manager->output_run = output_run;
		if (task_manager->is_global_last_merge == true) {
			global_last_merge = true;
			//print_time("start global last merge");
		} else {
			//print_time("start local last merge");
		}
	}

	for (;;) {
		if (heap.size() == 0) {
			break;
		}

		/* Get min */
		struct pair pair = heap.front();

		/* Write a tuple */
		if (global_last_merge == true) {
			/* Write to disk */
			output_run->full_flush_next(pair.ptr);
		} else if (local_last_merge == true) {
			/* Write some parts to disk */
			output_run->part_flush_next(pair.ptr);
		} else {
			/* Write to mem */
			output_run->write_next(pair.ptr);
		}

		/* Pop */
		std::pop_heap(heap.begin(), heap.end(), compare_pair());
		heap.pop_back();

		pair.ptr = run_vector->at(pair.num)->read_next();
		if (pair.ptr == NULL) {
			continue;
		}

		/* Push */
		heap.push_back(pair);
		std::push_heap(heap.begin(), heap.end(), compare_pair());
	}

	if (local_last_merge == true) {
		if (task_manager->flush_point == task_manager->output_run_len) {
			/* It is not make a temp file */
			task_manager->done = true;
		}
	}

	if (local_last_merge == false) {
		for (std::vector<Run*>::iterator iter = run_vector->begin(); iter != run_vector->end(); iter++) {
			delete *iter;
		}
		/* insert in run list */
		task_manager->insert_run(output_run);

		/* Insert merge sort task */
		make_merge_sort_task(task_manager);
	}
}

void make_merge_sort_task(TaskManager* task_manager) {
	Task* task = new Task();
	task_merge_sort_args* args = (task_merge_sort_args*) malloc(sizeof(task_merge_sort_args));
	args->task_manager = task_manager;
	task->make_task(TASK_MERGE_SORT, args);
	thread_pool->insert_task(task);
}

//----------------------------------------------------------------
//----------------------------------------------------------------

/* Flush a chunk to output file */
void flush_task(TaskManager* task_manager, Chunk* chunk, int64_t offset) {

	assert(offset + chunk->len <= task_manager->output_run_len);

	if (task_manager->is_global_last_merge == true) {
		/* Write this chunk to the output file */
		write_output->write_chunk(chunk, offset);
	} else {
		/* Write to temp file */
		task_manager->temp_file->write_chunk(chunk, offset);
	}

	int64_t counter = __sync_add_and_fetch(&task_manager->write_counter, chunk->len);

	/* This chunk have been used and isn't needed any more. Free it */
	chunk_manager->free_chunk(chunk);

	mb();
	if (counter == task_manager->output_run_len && task_manager->is_global_last_merge == true) {
		/* All flush is done */
		//write_output->end();
		print_time("flush end");
		printf("\n");
		exit(0);
	}
	mb();
	if (counter == task_manager->output_run_len - task_manager->flush_point) {
		task_manager->done = true;
	}
}

void make_flush_task(TaskManager* task_manager, Chunk* chunk, int64_t offset) {
	Task* task;
	task = new Task();
	task_flush_args* args = (task_flush_args*) malloc(sizeof(task_flush_args));
	args->task_manager = task_manager;
	args->chunk = chunk;
	args->offset = offset;
	task->make_task(TASK_FLUSH, args);
	thread_pool->insert_task(task);
}

//----------------------------------------------------------------
//----------------------------------------------------------------

/* Prefetch a chunk for last merge */
void prefetch_task(TaskManager* task_manager, int64_t offset, int64_t len) {

//	Chunk* chunk = task_manager->temp_file->read_chunk(offset, CHUNK_SIZE);
//
//	task_manager->output_run->chunks[index] = chunk;

//	print_time("Prefetch thread on");
//	char buffer[512];
//
//	for (int64_t i = 0; i + 512 < read_input->file_size; i += 4096) {
//		memcpy(write_output->file_map + i, buffer, 512);
//	}
//
//	print_time("Prefetch thread off");

	char buffer[512];
	for (int64_t i = 0; i < len; i += 512) {
		memcpy(buffer, write_output->file_map + offset + i, 1);
		//memcpy(write_output->file_map + offset + i, buffer, 1);
	}

	int64_t counter = __sync_add_and_fetch(&task_manager->prefetch_counter, len);
	if (counter == task_manager->output_run_len) {
		print_time("prefetch is done");
		task_manager->done = true;
		mb();
		task_manager->done_condition.notify_one();
	}

}

void make_prefetch_task(TaskManager* task_manager, int64_t offset, int64_t len) {
	Task* task;
	task = new Task();
	task_prefetch_args* args = (task_prefetch_args*) malloc(sizeof(task_prefetch_args));
	args->task_manager = task_manager;
	args->offset = offset;
	args->len = len;
	task->make_task(TASK_PREFETCH, args);
	thread_pool->insert_task(task);
}

//----------------------------------------------------------------
//----------------------------------------------------------------




Task::Task() {

}

Task::~Task() {

//	if (this->task == TASK_SAMPLE) {
//		struct task_sample_args* args = (struct task_sample_args*) this->args;
//		free(args);
//	} else if (this->task == TASK_READ_INPUT) {
//		struct task_read_input_args* args = (struct task_read_input_args*) this->args;
//		free(args);
//	} else if (this->task == TASK_QSORT_CHUNK) {
//		struct task_qsort_chunk_args* args = (struct task_qsort_chunk_args*) this->args;
//		free(args);
//	} else if (this->task == TASK_MERGE_SORT) {
//		struct task_merge_sort_args* args = (struct task_merge_sort_args*) this->args;
//		free(args);
//	} else if (this->task == TASK_FLUSH) {
//		struct task_flush_args* args = (struct task_flush_args*) this->args;
//		free(args);
//	} else if (this->task == TASK_PREFETCH) {
//		struct task_prefetch_args* args = (struct task_prefetch_args*) this->args;
//		free(args);
//	}
	free(args);
}


void Task::make_task(int task, void* args) {

	this->task = task;
	this->args = args;

}


void Task::run() {

	if (this->task == TASK_SAMPLE) {
		struct task_sample_args* args = (struct task_sample_args*) this->args;
		sample_task(args->a, args->b);
	} else if (this->task == TASK_READ_INPUT) {
		struct task_read_input_args* args = (struct task_read_input_args*) this->args;
		read_input_task(args->task_manager, args->offset, args->len);
	} else if (this->task == TASK_QSORT_CHUNK) {
		struct task_qsort_chunk_args* args = (struct task_qsort_chunk_args*) this->args;
		qsort_chunk_task(args->task_manager, args->chunk);
	} else if (this->task == TASK_MERGE_SORT) {
		struct task_merge_sort_args* args = (struct task_merge_sort_args*) this->args;
		merge_sort_task(args->task_manager);
	} else if (this->task == TASK_FLUSH) {
		struct task_flush_args* args = (struct task_flush_args*) this->args;
		flush_task(args->task_manager, args->chunk, args->offset);
	} else if (this->task == TASK_PREFETCH) {
		struct task_prefetch_args* args = (struct task_prefetch_args*) this->args;
		prefetch_task(args->task_manager, args->offset, args->len);
	} else if (this->task == TASK_SAMPLING) {
		struct task_sampling_args* args = (struct task_sampling_args*) this->args;
		sampling_task(args->task_manager, args->chunk_num);
	} else if (this->task == TASK_PARTITIONING) {
		struct task_partitioning_args* args = (struct task_partitioning_args*) this->args;
		partitioning_task(args->task_manager, args->tid, args->offset, args->len);
	} else if (this->task == TASK_PART_IN_MEM) {
		struct task_part_in_mem_args* args = (struct task_part_in_mem_args*) this->args;
		part_in_mem_task(args->task_manager, args->offset, args->len);
	} else if (this->task == TASK_PART) {
		struct task_part_args* args = (struct task_part_args*) this->args;
		part_task(args->task_manager, args->chunk);
	} else if (this->task == TASK_SORT_PART) {
		struct task_sort_part_args* args = (struct task_sort_part_args*) this->args;
		sort_part_task(args->task_manager, args->tid);
	} else if (this->task == TASK_FLUSH_ALL_PART) {
		struct task_flush_all_part_args* args = (struct task_flush_all_part_args*) this->args;
		flush_all_part_task(args->task_manager);
	} else if (this->task == TASK_FLUSH_ONE_PART) {
		struct task_flush_one_part_args* args = (struct task_flush_one_part_args*) this->args;
		flush_one_part_task(args->task_manager, args->part_index);
	}
}













