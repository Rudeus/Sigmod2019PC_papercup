

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <sys/stat.h>

#include <ctime>

#include <thread>


#include <unistd.h>
#include <time.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <iomanip>
#include <thread>
#include <jemalloc/jemalloc.h>




#include <Config.hpp>

#include <ThreadPool.hpp>
#include <ReadInput.hpp>
#include <WriteOutput.hpp>
#include <Chunk.hpp>
#include <Task.hpp>
#include <RunManager.hpp>


int64_t global_time_start;

// extern
int64_t hist[HIST_RANGE];
int PART_NUM;
int make_finish = 0;
int64_t* sort_chunk_num;
int64_t* sort_end_num;
int* ext_flag;
int part_id = 0;
int* part_point;
int64_t* thread_part_size[READ_THREAD_NUM];
int64_t* part_size;
bool* last_merge;
uint64_t* g_write_counter;
uint64_t total_flush_size = 0;
uint64_t total_file_size = 0;
int inmemory_part_num = 0;
int do_inmemory_part_num = 0;
bool* overlap_flag;

// global
const int kTupleSize = 100;
const int kKeySize = 10;

std::string g_outfile;

void print_time(const char str[]) {

	int64_t time_end = time(NULL);
	printf("%4d s: %s\n", time_end - global_time_start, str);

	//std::cout << std::setw(4) <<  time_end - global_time_start << " s : " << str << "\n";
}

void my_sort(const std::string &infile, const std::string &outfile) {

	g_outfile = outfile;
	struct stat st;
	stat(infile.data(), &st);

	if (st.st_size >= MEMSIZE) {
		PART_NUM = st.st_size / EXPECT_PARTITION;
		if (st.st_size % EXPECT_PARTITION != 0) PART_NUM++;

		int64_t cnt = 0;
		for (int i = 1; i <= PART_NUM; i++) {
			cnt += EXPECT_PARTITION;
			if (cnt >= MEMSIZE / 3 * 2) {
				inmemory_part_num = i;
				printf("inmemory part num: %d\n", inmemory_part_num);
				break;
			}
		}

		do_inmemory_part_num = inmemory_part_num;
	} else {
		printf("inmem sort\n");
		PART_NUM = st.st_size / INMEM_EXPECT_PARTITION;
		if (st.st_size % INMEM_EXPECT_PARTITION != 0) PART_NUM++;
		inmemory_part_num = PART_NUM;
	}

	bool* part_flag = (bool*)malloc(sizeof(bool) * (PART_NUM - 1));
	part_size = (int64_t*)malloc(sizeof(int64_t) * PART_NUM);
	g_write_counter = (uint64_t*)malloc(sizeof(uint64_t) * PART_NUM);
	sort_chunk_num = (int64_t*)malloc(sizeof(int64_t) * PART_NUM);
	sort_end_num = (int64_t*)malloc(sizeof(int64_t) * PART_NUM);
	ext_flag = (int*)malloc(sizeof(int) * PART_NUM);
	last_merge = (bool*)malloc(sizeof(bool) * PART_NUM);
	part_point = (int*)malloc(sizeof(int) * (PART_NUM - 1));

	if (inmemory_part_num == PART_NUM)
		memset(hist, 0, sizeof(int64_t) * INMEM_HIST_RANGE);
	else
		memset(hist, 0, sizeof(int64_t) * HIST_RANGE);
	memset(part_size, 0, sizeof(int64_t) * PART_NUM);
	memset(g_write_counter, 0, sizeof(uint64_t) * PART_NUM);
	memset(sort_chunk_num, 0, sizeof(int64_t) * PART_NUM);
	memset(sort_end_num, 0, sizeof(int64_t) * PART_NUM);
	memset(ext_flag, 0, sizeof(int) * PART_NUM);
	memset(last_merge, 0, sizeof(bool) * PART_NUM);
	memset(part_point, 0, sizeof(int) * (PART_NUM - 1));


	if (st.st_size >= MEMSIZE) {
		for (int i = 0; i < PART_NUM; i++)
			ext_flag[i] = 1;
	}

	/* Thread pool */
	thread_pool = new ThreadPool(WORKER_THREAD_NUM);

	read_input = new ReadInput(infile);

	write_output = new WriteOutput(outfile); /* must after alloc read input */

	chunk_manager = new ChunkManager(read_input->file_size);

	run_managers.resize(PART_NUM, NULL);	
	for (int i = 0; i < PART_NUM; i++) {
		run_managers[i] = new RunManager();
	}


	print_time("insert first task");
	//printf("HIST_CHUNK_SIZE: %lld\n", HIST_CHUNK_SIZE);

	if (ext_flag[part_id])	{	//dataset size > memmory size
		for (int i = 0; i < READ_THREAD_NUM; i++) {
			thread_part_size[i] = (int64_t*)malloc(sizeof(int64_t) * (PART_NUM - inmemory_part_num));
			memset(thread_part_size[i], 0, sizeof(int64_t) * (PART_NUM - inmemory_part_num));
		}
		overlap_flag = (bool*)malloc(sizeof(bool) * PART_NUM);
		memset(overlap_flag, true, sizeof(bool) * PART_NUM);
		printf("PART_NUM: %d\n", PART_NUM);
		//random read
		int64_t med = 0;
		//bool *chkdup;
		//chkdup = (bool*)malloc(sizeof(bool) * (st.st_size / HIST_CHUNK_SIZE));
		//memset(chkdup, 0, sizeof(chkdup));
		int64_t part_key = HIST_CHUNK_SIZE * HIST_CHUNK_NUM / 100 / PART_NUM;
		//printf("part_key: %lld\n", part_key);

		srand(time(NULL));

		Chunk* histchunk[HIST_CHUNK_NUM];

		//int64_t loc = rand() % ((st.st_size / HIST_CHUNK_SIZE) - HIST_CHUNK_NUM);
		// make histogram using sampling
		for (int64_t i = 0; i < HIST_CHUNK_NUM; i++) {
			int64_t loc = rand() % (st.st_size / HIST_CHUNK_SIZE / HIST_CHUNK_NUM);
			//while(chkdup[loc]) {
			//	loc = rand() % (st.st_size / HIST_CHUNK_SIZE);
			//}
			//chkdup[loc] = true;
			histchunk[i] = new Chunk(HIST_CHUNK_SIZE);
			Task* task = new Task();
			task_make_hist_args* args = (task_make_hist_args*)malloc(sizeof(task_make_hist_args));
			args->location = loc + 	st.st_size / HIST_CHUNK_SIZE / HIST_CHUNK_NUM * i;
			//args->location = loc + i;
			args->chunk = histchunk[i];
			task->make_task(TASK_MAKE_HIST, args);
			thread_pool->insert_task(task);
		}
	
		while(make_finish != HIST_CHUNK_NUM) {
			usleep(100);
		}

		for (int i = 0; i < HIST_CHUNK_NUM; i++) delete histchunk[i];

		for (int i = 0; i < HIST_RANGE; i++) {
			med += hist[i];
			for (int j = 1; j <= PART_NUM; j++) {
				if (med > part_key * j && !part_flag[j - 1]) {
					if (j != 1 && part_point[j - 2] != i - 1)
						part_point[j - 1] = i - 1;
					else
						part_point[j - 1] = i;

					part_flag[j - 1] = true;

					break;
				}
			}
		}
		//part_point[0] = 5126229;
		//part_point[1] = 8800000;
		//part_point[2] = 8851366;
		//part_point[3] = 11400000;
		for (int i = 0; i < PART_NUM - 1; i++)
			printf("partition point: %d\n", part_point[i]);
		//free(chkdup);

		int64_t read_range = st.st_size / READ_THREAD_NUM;
		int64_t iter_num = read_range / HIST_CHUNK_SIZE;
		if (iter_num * HIST_CHUNK_SIZE != read_range)	iter_num++;

		print_time("task propagation start");

		Chunk* partchunk[READ_THREAD_NUM];

		// first in-memory partition (part_id = 0)
		for (int i = 0; i < READ_THREAD_NUM; i++) { 
			partchunk[i] = new Chunk(HIST_CHUNK_SIZE);
			Task* task = new Task();
			task_make_partition_args* args = (task_make_partition_args*)malloc(sizeof(task_make_partition_args));
			//args->location = i * HIST_CHUNK_SIZE;
			args->tid = i;
			args->location = i * read_range;
			//printf("args->location: %lld\n", args->location);
			args->iter_num = iter_num;
			args->chunk = partchunk[i];
			args->id = part_id;
			task->make_task(TASK_MAKE_PARTITION, args);
			thread_pool->insert_task(task);
		}

		while (1){
			//if (ext_flag[part_id] == READ_THREAD_NUM + 1 && g_write_counter[part_id] == part_size[part_id]) break;
			//if ((g_write_counter[part_id] >= part_size[part_id] / 3 * 2 && ext_flag[part_id] == READ_THREAD_NUM + 1) || do_inmemory_part_num < inmemory_part_num)  {
			if (do_inmemory_part_num < inmemory_part_num && ext_flag[part_id] == READ_THREAD_NUM + 1) {
				//close(read_input->fd);
				//printf("first overlap start\n");
				break;
			}
			usleep(100);
		}

		for (int i = 0; i < READ_THREAD_NUM; i++) delete partchunk[i];
		
		print_time("first partition flush");
		
		for (int id = inmemory_part_num; id < PART_NUM; id++) {

		// second partition sort-merge
			part_id = id;
			do_inmemory_part_num++;
			for (int i = 0; i < READ_THREAD_NUM; i++) {
				Task* task = new Task();
				task_partition_sort_args* args = (task_partition_sort_args*)malloc(sizeof(task_partition_sort_args));
				args->tid = i;
				args->id = part_id;
				task->make_task(TASK_PARTITION_SORT, args);
				thread_pool->insert_task(task);
			}

			if (id == PART_NUM - 1) {
				printf("break!\n");
				break;
			}
				
			while (1){
				//if (g_write_counter[part_id] == part_size[part_id] && ext_flag[part_id] == READ_THREAD_NUM + 1) break;
				if (do_inmemory_part_num < inmemory_part_num && ext_flag[part_id] == READ_THREAD_NUM + 1) {
					//printf("overlap start\n");
					break;
				}
				usleep(100);
			}
		}
		
		while (1){
			if (ext_flag[part_id] == READ_THREAD_NUM + 1 && g_write_counter[part_id] == part_size[part_id]) break;
			usleep(100);
		}
		
		//printf("total_flush_size: %lld\ntotal_file_size: %lld\n", total_flush_size, total_file_size);
		for (;;) {
			if (total_flush_size == total_file_size) {
				/*free(part_point);
				free(part_size);
				free(g_write_counter);
				free(sort_chunk_num);
				free(sort_end_num);
				free(ext_flag);
				free(last_merge);
				free(overlap_flag);
				free(part_flag);
				for (int i = 0; i < READ_THREAD_NUM; i++)
					free(thread_part_size[i]);

				// After all flush is done 
				write_output->end();*/
				print_time("end");
				/*int64_t time_end = time(NULL);
				for (;;) {
					if (time_end - global_time_start > 380) {
						print_time("time out :(");
						exit(0);
					}
				}*/

				exit(0);
			}
			usleep(100);
		}
	} else {
		printf("PART_NUM: %d\n", PART_NUM);
		//random read
		/*int64_t med = 0;
		bool *chkdup;
		chkdup = (bool*)malloc(sizeof(bool) * (st.st_size / HIST_CHUNK_SIZE));
		memset(chkdup, 0, sizeof(chkdup));
		int64_t part_key = HIST_CHUNK_SIZE * INMEM_HIST_CHUNK_NUM / 100 / PART_NUM;
		//printf("part_key: %lld\n", part_key);

		srand(time(NULL));

		Chunk* histchunk[INMEM_HIST_CHUNK_NUM];

		// make histogram using sampling
		for (int64_t i = 0; i < INMEM_HIST_CHUNK_NUM; i++) {
			int64_t loc = rand() % (st.st_size / HIST_CHUNK_SIZE);
			while(chkdup[loc]) {
				loc = rand() % (st.st_size / HIST_CHUNK_SIZE);
			}
			chkdup[loc] = true;
			histchunk[i] = new Chunk(HIST_CHUNK_SIZE);
			Task* task = new Task();
			task_make_hist_args* args = (task_make_hist_args*)malloc(sizeof(task_make_hist_args));
			args->location = loc;
			args->chunk = histchunk[i];
			task->make_task(TASK_MAKE_HIST, args);
			thread_pool->insert_task(task);
		}
	
		while(make_finish != INMEM_HIST_CHUNK_NUM) {
			usleep(100);
		}

		for (int i = 0; i < INMEM_HIST_CHUNK_NUM; i++) delete histchunk[i];

		for (int i = 0; i < HIST_RANGE; i++) {
			med += hist[i];
			for (int j = 1; j <= PART_NUM; j++) {
				if (med > part_key * j && !part_flag[j - 1]) {
					if (j != 1 && part_point[j - 2] != i - 1)
						part_point[j - 1] = i - 1;
					else
						part_point[j - 1] = i;

					part_flag[j - 1] = true;

					break;
				}
			}
		}

		//for (int i = 0; i < PART_NUM - 1; i++)
		//	printf("partition point: %d\n", part_point[i]);
		free(chkdup);*/

		int start_point = 31, part = 12;
		for (int i = 1; i < PART_NUM; i++) {
			if (st.st_size == SMALL_SET) {
				part_point[i - 1] = INMEM_HIST_RANGE / PART_NUM * i;
			}
			else {
				start_point += part;
				//if (i % 3 == 0 || i == 1)	start_point--;
				part_point[i - 1] = start_point;
			}
		}

		int64_t read_range = st.st_size / READ_THREAD_NUM;
		int64_t iter_num = read_range / HIST_CHUNK_SIZE;
		if (iter_num * HIST_CHUNK_SIZE != read_range)	iter_num++;

		print_time("task propagation start");

		Chunk* partchunk[READ_THREAD_NUM];

		// first in-memory partition (part_id = 0)
		for (int i = 0; i < READ_THREAD_NUM; i++) { 
			partchunk[i] = new Chunk(HIST_CHUNK_SIZE);
			Task* task = new Task();
			task_make_partition_args* args = (task_make_partition_args*)malloc(sizeof(task_make_partition_args));
			args->tid = i;
			args->location = i * read_range;
			args->iter_num = iter_num;
			args->chunk = partchunk[i];
			args->id = part_id;
			task->make_task(TASK_MAKE_PARTITION, args);
			thread_pool->insert_task(task);
		}

		while (1){
			if (do_inmemory_part_num == inmemory_part_num) {
				/*for (int i = 0; i < READ_THREAD_NUM; i++) delete partchunk[i];
				close(read_input->fd);
				free(part_size);
				free(g_write_counter);
				free(sort_chunk_num);
				free(sort_end_num);
				free(ext_flag);
				free(last_merge);
				free(part_point);
				free(part_flag);*/

				print_time("end");
				exit(0);
				break;
			}
			usleep(100);
		}

	}
}


void timer_thread_func(void) {

	for (;;) {
		usleep(10000000); // 10sec
		//print_time("10 sec!");
		int64_t time_end = time(NULL);
		if (time_end - global_time_start > 410) {
			print_time("time out :(");
			exit(0);
			break;
		}
	}

	return;
}



int main(int argc, char *argv[]) {
	if (argc != 3) {
		std::cout << "USAGE: " << argv[0] << " [in-file] [outfile]\n";
		std::exit(-1);
	}

	global_time_start = time(NULL);

	printf("chunk size : %lld M\n", CHUNK_SIZE / 1024 / 1024);
	printf("chunk num : %lld\n", CHUNK_NUM);
	printf("total chunk size : %lld G\n", CHUNK_SIZE * CHUNK_NUM / 1024 / 1024 / 1024);
	printf("thread num : %d\n", WORKER_THREAD_NUM);
	printf("read thread num : %d\n", READ_THREAD_NUM);
	printf("merge way num : %d\n", MERGE_WAY_NUM);

	print_time("start");

	//std::thread timerthread(&timer_thread_func);


	my_sort(argv[1], argv[2]);

	for (;;) {
		usleep(10000);
	}

	return 0;
}

