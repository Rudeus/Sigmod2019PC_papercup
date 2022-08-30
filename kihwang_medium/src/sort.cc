

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
#include <mutex>
#include <condition_variable>


#include <string.h>
#include <unistd.h>
#include <math.h>
#include <jemalloc/jemalloc.h>
#include <assert.h>




#include <Config.hpp>

#include <ThreadPool.hpp>
#include <ReadInput.hpp>
#include <WriteOutput.hpp>
#include <Chunk.hpp>
#include <ChunkManager.hpp>
#include <Task.hpp>
#include <TaskManager.hpp>
#include <TempFile.hpp>




int64_t global_time_start;


const int kTupleSize = 100;
const int kKeySize = 10;






void print_time(const char str[]) {

	int64_t time_end = time(NULL);

	printf("%4ld s : %s\n", time_end - global_time_start, str);
}

void sort(const std::string &infile, const std::string &outfile) {
	std::ifstream inputstream(infile);
	if (!inputstream) {
		std::cerr << "Could not open the file\n";
		std::exit(-1);
	}

	// get size of file
	inputstream.seekg(0, inputstream.end);
	const std::int64_t size = inputstream.tellg();
	inputstream.seekg(0);

	// allocate memory for file content
	const std::int64_t num_tuples = size / kTupleSize;
	std::vector<std::array<char, kTupleSize>> buffer(num_tuples);

	std::ofstream outputstream(outfile);

	// read content of is
	for (std::int64_t i = 0; i < num_tuples; ++i) {
		inputstream.read(buffer[i].data(), kTupleSize);
	}

	std::sort(buffer.begin(), buffer.end(),
			[](const std::array<char, kTupleSize> &lhs,
				const std::array<char, kTupleSize> &rhs) {
			for (int i = 0; i < kKeySize; ++i) {
				if (lhs[i] == rhs[i]) {
					continue;
				}
				return unsigned(lhs[i]) < unsigned(rhs[i]);
			}

			return false;
			});

	for (std::int64_t i = 0; i < num_tuples; ++i) {
		outputstream.write(buffer[i].data(), kTupleSize);
	}
	inputstream.close();
	outputstream.close();
}


void do_in_memory() {
	print_time("do in memory");

	int64_t file_size = chunk_manager->file_size;

	TaskManager* task_manager = new TaskManager();
	task_manager->output_run_len = file_size;
	task_manager->is_global_last_merge = true;

	int64_t sep = file_size / CHUNK_SIZE / IO_THREAD_NUM;
	for (int64_t i = 0; i < IO_THREAD_NUM; i++) {
		if (i == IO_THREAD_NUM - 1)
			task_manager->merge_from_input(i * sep * CHUNK_SIZE, file_size - i * sep * CHUNK_SIZE);
		else
			task_manager->merge_from_input(i * sep * CHUNK_SIZE, sep * CHUNK_SIZE);
	}

	for (;;) {
		usleep(1000000);
		int64_t time_end = time(NULL);
		if (time_end - global_time_start > 160) {
			print_time("time out :(");
			break;
		}
	}

	exit(0);
}

void do_small() {
	print_time("do small");


	int64_t file_size = chunk_manager->file_size;

	int64_t big_chunk_num = 10;
	int64_t sub_run_size = (1000LL * 1000 * 1000); /* 1000M */
	std::vector<TaskManager*> tm_vector(big_chunk_num);
	for (int64_t i = 0; i < big_chunk_num; i++) {

		tm_vector[i] = new TaskManager();
		tm_vector[i]->output_run_len = sub_run_size;
		tm_vector[i]->flush_point = sub_run_size;
		tm_vector[i]->merge_way_num = MERGE_WAY_NUM;

		int64_t sep;

		sep = tm_vector[i]->output_run_len / CHUNK_SIZE / IO_THREAD_NUM;
		for (int64_t j = 0; j < IO_THREAD_NUM; j++) {
			if (j == IO_THREAD_NUM - 1)
				tm_vector[i]->merge_from_input(i * sub_run_size + j * sep * CHUNK_SIZE, tm_vector[i]->output_run_len - j * sep * CHUNK_SIZE);
			else
				tm_vector[i]->merge_from_input(i * sub_run_size + j * sep * CHUNK_SIZE, sep * CHUNK_SIZE);
		}


//		if (i - 4 >= 0) {
//			for (;;) {
//				usleep(1000);
//				if (tm_vector[i - 4]->done == true) {
//					print_time("----- local merge is done -----");
//					break;
//				}
//				int64_t time_end = time(NULL);
//				if (time_end - global_time_start > 110) {
//					print_time("time out :(");
//					exit(0);
//					break;
//				}
//			}
//		}
	}

	for (int64_t i = 0; i < big_chunk_num; i++) {
		for (;;) {
			usleep(100000);
			if (tm_vector[i]->done == true) {
				break;
			}
			int64_t time_end = time(NULL);
			if (time_end - global_time_start > 110) {
				print_time("time out :(");
				exit(0);
				break;
			}
		}
	}


	std::vector<Run*> run_vector(big_chunk_num);
	for (int64_t i = 0; i < big_chunk_num; i++) {
		run_vector[i] = tm_vector[i]->output_run;
	}



	TaskManager* task_manager_final = new TaskManager();
	task_manager_final->output_run_len = file_size;
	task_manager_final->is_global_last_merge = true;
	task_manager_final->merge_way_num = MERGE_WAY_NUM;

	task_manager_final->merge_from_output_run(run_vector);



	for (;;) {
		usleep(1000000);
		int64_t time_end = time(NULL);
		if (time_end - global_time_start > 100) {
			print_time("time out :(");
			exit(0);
			break;
		}
	}

}


void do_medium() {
	print_time("do medium");


	int64_t file_size = chunk_manager->file_size;

	int64_t big_chunk_num = 25;
	int64_t sub_run_size = (1000LL * 1000 * 800); /* 800M */
	std::vector<TaskManager*> tm_vector(big_chunk_num);
	for (int64_t i = 0; i < big_chunk_num; i++) {

		tm_vector[i] = new TaskManager();
		tm_vector[i]->output_run_len = sub_run_size;
		tm_vector[i]->flush_point = sub_run_size;
		tm_vector[i]->merge_way_num = MERGE_WAY_NUM;

		int64_t sep;

		sep = tm_vector[i]->output_run_len / CHUNK_SIZE / IO_THREAD_NUM;
		for (int64_t j = 0; j < IO_THREAD_NUM; j++) {
			if (j == IO_THREAD_NUM - 1)
				tm_vector[i]->merge_from_input(i * sub_run_size + j * sep * CHUNK_SIZE, tm_vector[i]->output_run_len - j * sep * CHUNK_SIZE);
			else
				tm_vector[i]->merge_from_input(i * sub_run_size + j * sep * CHUNK_SIZE, sep * CHUNK_SIZE);
		}


		if (i - 7 >= 0) {
			for (;;) {
				usleep(1000);
				if (tm_vector[i - 7]->done == true) {
					//print_time("----- local merge is done -----");
					break;
				}
				int64_t time_end = time(NULL);
				if (time_end - global_time_start > 160) {
					print_time("time out :(");
					exit(0);
					break;
				}
			}
		}
	}

	for (int64_t i = 0; i < big_chunk_num; i++) {
		for (;;) {
			usleep(100000);
			if (tm_vector[i]->done == true) {
				break;
			}
			int64_t time_end = time(NULL);
			if (time_end - global_time_start > 160) {
				print_time("time out :(");
				exit(0);
				break;
			}
		}
	}


	std::vector<Run*> run_vector(big_chunk_num);
	for (int64_t i = 0; i < big_chunk_num; i++) {
		run_vector[i] = tm_vector[i]->output_run;
	}



	TaskManager* task_manager_final = new TaskManager();
	task_manager_final->output_run_len = file_size;
	task_manager_final->is_global_last_merge = true;
	task_manager_final->merge_way_num = MERGE_WAY_NUM;

	task_manager_final->merge_from_output_run(run_vector);



	for (;;) {
		usleep(1000000);
		int64_t time_end = time(NULL);
		if (time_end - global_time_start > 160) {
			print_time("time out :(");
			exit(0);
			break;
		}
	}

}


void do_large() {
	print_time("do large");


	int64_t file_size = chunk_manager->file_size;

	std::string path = write_output->outfile;
	std::size_t point = path.find_last_of("/");
	std::string dir = path.substr(0, point);
	std::string file = path.substr(point + 1, path.length());

	TaskManager* tm = new TaskManager();
	tm->sort_type = EXT_PART_SORT;
	tm->prefix_size = 8;
	tm->make_histogram(20, 6);

	int64_t partition_size = (int64_t) (1000LL * 1000 * 1000 * 10);
	tm->partitioning(30, partition_size, 2);

	for (int64_t i = 0; i < tm->partition_num; i++) {
		tm->merge_partition(i);
		break;
	}


	int64_t big_chunk_num = 30;
	std::vector<TempFile*> tf_vector(big_chunk_num);
	std::vector<TaskManager*> tm_vector(big_chunk_num);
	for (int64_t i = 0; i < big_chunk_num; i++) {

		std::string filename = path + std::to_string(i);
		tf_vector[i] = new TempFile(filename, PART_FILE_SIZE);
		tm_vector[i] = new TaskManager();
		tm_vector[i]->temp_file = tf_vector[i];
		tm_vector[i]->output_run_len = SUB_RUN_SIZE;
		tm_vector[i]->flush_point = SUB_RUN_SIZE - PART_FILE_SIZE;
		tm_vector[i]->merge_way_num = MERGE_WAY_NUM;

		int64_t sep;

		sep = tm_vector[i]->output_run_len / CHUNK_SIZE / IO_THREAD_NUM;
		for (int64_t j = 0; j < IO_THREAD_NUM; j++) {
			if (j == IO_THREAD_NUM - 1)
				tm_vector[i]->merge_from_input(i * SUB_RUN_SIZE + j * sep * CHUNK_SIZE, tm_vector[i]->output_run_len - j * sep * CHUNK_SIZE);
			else
				tm_vector[i]->merge_from_input(i * SUB_RUN_SIZE + j * sep * CHUNK_SIZE, sep * CHUNK_SIZE);
		}


		if (i - 3 >= 0) {
			for (;;) {
				usleep(1000);
				if (tm_vector[i - 3]->done == true) {
					print_time("----- local merge is done -----");
					break;
				}
				int64_t time_end = time(NULL);
				if (time_end - global_time_start > 400) {
					print_time("time out :(");
					exit(0);
					break;
				}
			}
		}
	}

	for (int64_t i = 0; i < big_chunk_num; i++) {
		for (;;) {
			usleep(1000);
			if (tm_vector[i]->done == true) {
				break;
			}
			int64_t time_end = time(NULL);
			if (time_end - global_time_start > 400) {
				print_time("time out :(");
				exit(0);
				break;
			}
		}
	}


	std::vector<Run*> run_vector(big_chunk_num);
	for (int64_t i = 0; i < big_chunk_num; i++) {
		run_vector[i] = tm_vector[i]->output_run;
	}



	TaskManager* task_manager_final = new TaskManager();
	task_manager_final->output_run_len = file_size;
	task_manager_final->is_global_last_merge = true;
	task_manager_final->merge_way_num = MERGE_WAY_NUM;

	task_manager_final->merge_from_output_run(run_vector);


	for (;;) {
		usleep(1000000);
		int64_t time_end = time(NULL);
		if (time_end - global_time_start > 400) {
			print_time("time out :(");
			exit(0);
			break;
		}
	}

}


int64_t key_to_index(char* key) {
	int64_t index = 0;
	for (int64_t i = 0; i < KEYSIZE; i++) {
		index += (key[i] - 32) * pow(100, (KEYSIZE - i - 1));
	}
	return index;
}


void for_ascii(void) {

	Chunk* chunk = read_input->read_chunk(0, CHUNK_SIZE);

	for (int64_t i = 0; i < chunk->len; i += TUPLESIZE) {
		printf("%lu -> %lu\n", i, key_to_index(chunk->buffer + i));
		break;
	}

	exit(0);
}

int workload_type;
enum {SMALL, MEDIUM};

/* For ascii */
int64_t key2pid(char* key) {

	if (workload_type == SMALL) {
		uint64_t index = __builtin_bswap64(*((uint64_t*) key));
		index = index >> (64 - 10);

		return index;

	} else if (workload_type == MEDIUM) {
		unsigned char p1 = (unsigned char) key[0];
		unsigned char p2 = (unsigned char) key[1];
		unsigned char p3 = (unsigned char) key[2];

		//int64_t pid = (p1 - 32) * 95 * 95 + (p2 - 32) * 95 + (p3 - 32);
		int64_t pid = ((p1 - 32) * 95 + (p2 - 32)) * 2 + (p3 >> 5);

		return pid;
	} else {
		return 0;
	}
}

void small_partition_and_radix_sort(void) {

	workload_type = SMALL;

	int64_t file_size = chunk_manager->file_size;

	/* Make space for partitions */
	TaskManager* tm = new TaskManager();
	char key[10];
	key[0] = 0xff; key[1] = 0xff; key[2] = 0xff;
	int64_t part_num = key2pid(key) + 1;
	printf("part number -> %lu\n", part_num);
	tm->part_num = part_num;
	tm->partition_buffers.resize(part_num, NULL);
	tm->partition_lens.resize(part_num, 0);
	tm->partition_sort_done.resize(part_num, 0);
	tm->partition_size = file_size * 5 / part_num / 4;
	tm->sort_thread_num = 9;
	tm->local_part_index.resize(tm->sort_thread_num, -1);
	for (int64_t i = 0; i < part_num; i++) {
		tm->partition_buffers[i] = (char*) malloc(tm->partition_size);
		//tm->partition_buffers[i] = write_output->file_map + tm->partition_size * i;
	}
	tm->output_run_len = file_size;

//	/* Prefetch task */
//	Task* task = new Task();
//	task_prefetch_args* args = (task_prefetch_args*) malloc(sizeof(task_prefetch_args));
//	task->make_task(TASK_PREFETCH, args);
//	thread_pool->insert_task(task);

	/* Make partitioning tasks */
	//make_part_in_mem_task(tm, 0, file_size); /* Read by only one thread */
	print_time("Read thread on");
	int64_t sep = file_size / CHUNK_SIZE / (IO_THREAD_NUM - 1);
	for (int64_t i = 0; i < IO_THREAD_NUM; i++) {
		if (i == IO_THREAD_NUM - 1) {
			make_part_in_mem_task(tm, i * sep * CHUNK_SIZE, file_size - i * sep * CHUNK_SIZE);
			make_prefetch_task(tm, i * sep * CHUNK_SIZE, file_size - i * sep * CHUNK_SIZE);
		} else {
			make_part_in_mem_task(tm, i * sep * CHUNK_SIZE, sep * CHUNK_SIZE);
			make_prefetch_task(tm, i * sep * CHUNK_SIZE, sep * CHUNK_SIZE);
		}
	}

	/* Wait until partitioning tasks are done */
	{
		std::unique_lock<std::mutex> lock(tm->done_lock);
		tm->done_condition.wait(lock, [tm]{ return tm->part_in_mem_counter == read_input->file_size && tm->prefetch_counter == read_input->file_size; });
	}
	tm->done = false;

	/* Free task */
//	Task* task = new Task();
//	task_sample_args* args2 = (task_sample_args*) malloc(sizeof(task_sample_args));
//	args2->a = 1;
//	args2->b = 2;
//	task->make_task(TASK_SAMPLE, args2);
//	thread_pool->insert_task(task);


	/* Offset of each partition */
	tm->part_offset.resize(part_num, 0);
	tm->part_offset[0] = 0;
	for (int64_t i = 1; i < part_num; i++) {
		tm->part_offset[i] = tm->part_offset[i - 1] + tm->partition_lens[i - 1];
	}

	/* Pulling */
//	for (int64_t i = 0; i < part_num; i++) {
//		memcpy(write_output->file_map + tm->part_offset[i], tm->partition_buffers[i], tm->partition_lens[i]);
//	}
//	print_time("pull done");


	/* Make a task for write */
	//make_flush_all_part_task(tm);

	/* Make tasks to sort and flush */
	print_time("Sort in the output file");
	for (int64_t i = 0; i < tm->sort_thread_num; i++) {
		make_sort_part_task(tm, i);
	}

	/* Wait until done */
	{
		std::unique_lock<std::mutex> lock(tm->done_lock);
		tm->done_condition.wait(lock, [tm]{ return tm->done; });
	}

	exit(0);
}

void medium_partition_and_radix_sort(void) {

	workload_type = MEDIUM;

	int64_t file_size = chunk_manager->file_size;

	/* Make space for partitions */
	TaskManager* tm = new TaskManager();
	char key[10];
	key[0] = 126; key[1] = 126; key[2] = 126;
	int64_t part_num = key2pid(key) + 1;
	printf("part number -> %lu\n", part_num);
	tm->part_num = part_num;
	tm->partition_buffers.resize(part_num, NULL);
	tm->partition_lens.resize(part_num, 0);
	tm->partition_sort_done.resize(part_num, 0);
	tm->partition_size = file_size * 50 / 4 / part_num;
	tm->sort_thread_num = 17;
	tm->local_part_index.resize(tm->sort_thread_num, -1);
	for (int64_t i = 0; i < part_num; i++) {
		tm->partition_buffers[i] = (char*) malloc(tm->partition_size);
		//tm->partition_buffers[i] = write_output->file_map + tm->partition_size * i;
	}
	tm->output_run_len = file_size;


	/* Make partitioning tasks */
	//make_part_in_mem_task(tm, 0, file_size); /* Read by only one thread */
	print_time("Read thread on");
	int64_t sep = file_size / CHUNK_SIZE / (IO_THREAD_NUM - 1);
	for (int64_t i = 0; i < IO_THREAD_NUM; i++) {
		if (i == IO_THREAD_NUM - 1) {
			make_part_in_mem_task(tm, i * sep * CHUNK_SIZE, file_size - i * sep * CHUNK_SIZE);
		} else {
			make_part_in_mem_task(tm, i * sep * CHUNK_SIZE, sep * CHUNK_SIZE);
		}
	}

	/* Prefetch tasks */
	tm->prefetch_len = file_size / 3;
	sep = tm->prefetch_len / CHUNK_SIZE / (IO_THREAD_NUM - 1);
	for (int64_t i = 0; i < IO_THREAD_NUM; i++) {
		if (i == IO_THREAD_NUM - 1) {
			make_prefetch_task(tm, i * sep * CHUNK_SIZE, tm->prefetch_len - i * sep * CHUNK_SIZE);
		} else {
			make_prefetch_task(tm, i * sep * CHUNK_SIZE, sep * CHUNK_SIZE);
		}
	}

	/* Wait until partitioning tasks are done */
	{
		std::unique_lock<std::mutex> lock(tm->done_lock);
		tm->done_condition.wait(lock, [tm]{ return tm->part_in_mem_counter == read_input->file_size && tm->prefetch_counter == tm->prefetch_len; });
	}
	tm->done = false;
	print_time("Read and prefetch are done");

	/* Offset of each partition */
	tm->part_offset.resize(part_num, 0);
	tm->part_offset[0] = 0;
	for (int64_t i = 1; i < part_num; i++) {
		tm->part_offset[i] = tm->part_offset[i - 1] + tm->partition_lens[i - 1];
	}

	/* Make a task for write */
	//make_flush_all_part_task(tm);

	/* Make tasks to sort and flush */
	print_time("Sort in the output file");
	for (int64_t i = 0; i < tm->sort_thread_num; i++) {
		make_sort_part_task(tm, i);
	}

	/* Wait until done */
	{
		std::unique_lock<std::mutex> lock(tm->done_lock);
		tm->done_condition.wait(lock, [tm]{ return tm->done; });
	}

	exit(0);
}

void my_sort(const std::string &infile, const std::string &outfile) {


	read_input = new ReadInput(infile);


	if (read_input->file_size == (1000LL * 1000 * 1000 * 10)) {
		/* Small */
		print_time("Small");

		/* Thread pool */
		thread_pool = new ThreadPool(WORKER_THREAD_NUM);

		write_output = new WriteOutput(outfile); /* must after alloc read input */

		chunk_manager = new ChunkManager(read_input->file_size);

		//do_small();
		//medium_partition_and_radix_sort();
		small_partition_and_radix_sort();

		exit(0);
	} else if (read_input->file_size == (1000LL * 1000 * 1000 * 20)) {
		/* Medium */
		print_time("Medium");

		/* Thread pool */
		thread_pool = new ThreadPool(WORKER_THREAD_NUM);

		write_output = new WriteOutput(outfile); /* must after alloc read input */

		chunk_manager = new ChunkManager(read_input->file_size);

		//do_small();
		medium_partition_and_radix_sort();

		//do_medium();
		//for_ascii();
		exit(0);
	} else if (read_input->file_size == (1000LL * 1000 * 1000 * 60)) {
		/* Large */
		print_time("Large");

		/* Thread pool */
		thread_pool = new ThreadPool(WORKER_THREAD_NUM);

		write_output = new WriteOutput(outfile); /* must after alloc read input */

		chunk_manager = new ChunkManager(read_input->file_size);

		exit(0);
		do_large();
	}



}



int main(int argc, char *argv[]) {
	if (argc != 3) {
		printf("USAGE INVALID\n");
		std::exit(-1);
	}

	/* Random seed */
	srand(10);

	/* Set start time */
	global_time_start = time(NULL);

	print_time("start");

	/* Let's start */
	my_sort(argv[1], argv[2]);

	for (;;) {
		usleep(10000000);
	}

	return 0;
}

