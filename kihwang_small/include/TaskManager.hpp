#pragma once


#include <list>
#include <thread>
#include <mutex>
#include <condition_variable>



#include <Run.hpp>



class TempFile;
class Run;
class PartFile;

/**
 * EXT_TEMP_SORT : 
 * EXT_PART_SORT : 
 */
enum { EXT_TEMP_SORT, EXT_PART_SORT, };

class TaskManager {

	public:

	/* Length of output run */
	int64_t output_run_len;

	/* Type of sort */
	int sort_type;


	/* Local run list sorted by length of run */
	std::list<Run*> run_list;
	std::mutex run_list_lock;
	int64_t len_in_run_list = 0;

	/* Flush all */
	bool is_global_last_merge = false;

	/* Merge way number */
	int64_t merge_way_num = MERGE_WAY_NUM;

	/* Partition in memory */
	int64_t part_in_mem_counter = 0;
	int64_t part_num = 0;
	std::vector<Run*> partition_runs;
	std::vector<int64_t> part_offset;
	std::vector<char*> partition_buffers;
	std::vector<int64_t> partition_lens;
	std::vector<int64_t> partition_sort_done;
	int64_t partition_size = 0;
	int64_t sort_thread_num = 0;
	int64_t lsn = 0;
	std::vector<int64_t> local_part_index;
	int64_t part_flush_counter = 0;
	int64_t prefetch_counter = 0;

	/* For histogram */
	int64_t prefix_size = 0;
	int64_t hist_index_num = 0; /* pow(2, prefix_size) */
	std::vector<int64_t> histogram;
	int64_t sampling_len = 0;
	int64_t sampling_counter = 0; /* For sync */
	std::vector<int64_t> partition_point;
	std::vector<int64_t> partition_map;
	int64_t partition_num = 0; /* The number of portitions */
	int64_t overlap = 0;
	std::vector<std::vector<PartFile*>> part_files;
	int64_t partition_counter = 0; /* For sync */
	std::vector<int64_t> real_part_size;

	/* Partition merge */
	int64_t partition_start_offset = 0;
	int64_t partition_len = 0;

	/* Temp file */
	TempFile* temp_file = NULL;
	int64_t flush_point = 0;

	/* Output */
	Run* output_run = NULL;

	/* For sync */
	int64_t read_counter = 0;
	int64_t write_counter = 0;

	/* This task set is done */
	bool done = false;

	std::condition_variable done_condition;
	std::mutex done_lock;


	public:
	TaskManager();
	~TaskManager();

	void make_histogram(int64_t thread_num, int64_t sampling_chunk_num);
	void partitioning(int64_t thread_num, int64_t partitoin_size, int64_t overlap);
	void merge_partition(int64_t partition_id);
	void merge_from_input(int64_t offset, int64_t len);
	void merge_from_output_run(std::vector<Run*> run_vector);
	void insert_run(Run* run);
	std::vector<Run*>* get_run_vector();


};














