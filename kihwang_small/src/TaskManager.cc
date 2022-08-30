

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <cmath>
#include <ctime>
#include <iomanip>


#include <assert.h>
#include <unistd.h>


#include <Config.hpp>




#include <ThreadPool.hpp>
#include <Task.hpp>
#include <Run.hpp>
#include <ReadInput.hpp>
#include <PartFile.hpp>


#include <TaskManager.hpp>






TaskManager::TaskManager() {


}



TaskManager::~TaskManager() {


}


/* Chunk number per thread */
void TaskManager::make_histogram(int64_t thread_num, int64_t sampling_chunk_num) {
	assert(prefix_size != 0);

	hist_index_num = pow(2, prefix_size);
	sampling_counter = 0;
	sampling_len = 0;

	/* Init histogram */
	histogram.resize(hist_index_num, 0);

	mb();

	/* Make sampling tasks */
	for (int64_t i = 0; i < thread_num; i++) {
		make_sampling_task(this, sampling_chunk_num);
	}

	/* Wait until sampling is done */
	for (;;) {
		usleep(1000);
		if (sampling_counter == thread_num) {
			print_time("----- sampling is done -----");
			break;
		}
	}

	/* Print histogram */
//	for (int64_t i = 0; i < hist_index_num; i++) {
//		std::cout << i << " : " << histogram[i] << "\n";
//	}
}

void TaskManager::partitioning(int64_t thread_num, int64_t partition_size, int64_t overlap) {
	int64_t sep = sampling_len / (read_input->file_size / partition_size) / TUPLESIZE;

	/* Make partition points */
	int64_t sum = 0;
	for (int64_t i = 0; i < hist_index_num; i++) {
		if (sum + histogram[i] <= sep && i != hist_index_num - 1) {
			sum += histogram[i];
		} else {
			sum = 0;
			partition_point.emplace_back(i + 1);
		}
	}
	partition_num = partition_point.size();

	/* Mapping prefix to partition number */
	partition_map.resize(hist_index_num);
	int64_t start = 0;
	for (int64_t i = 0; i < partition_num; i++) {
		for (int64_t j = start; j < partition_point[i]; j++) {
			partition_map[j] = i;
			start = partition_point[i];
		}
	}

	/* Print partition points */
	std::cout << partition_point.size() << "\n";
	for (int64_t i = 0; i < (int64_t) partition_point.size(); i++) {
		std::cout << partition_point[i] << "\n";
	}

	/* Partitioning */
	real_part_size.resize(partition_num, 0);
	int64_t size_per_thread = read_input->file_size / thread_num;
	this->overlap = overlap;
	part_files.resize(thread_num, std::vector<PartFile*>(partition_num, NULL));
	for (int64_t i = 0; i < thread_num; i++) {
		if (i == thread_num - 1)
			/* Last one */
			make_partitioning_task(this, i, i * size_per_thread, read_input->file_size - i * size_per_thread);
		else
			make_partitioning_task(this, i, i * size_per_thread, size_per_thread);
	}

	/* Wait until partitioning is done */
	for (;;) {
		usleep(1000);
		if (partition_counter == read_input->file_size) {
			print_time("----- partitioning is done -----");
			break;
		}
	}

	/* Print the size of each partition */
	for (int64_t i = 0; i < (int64_t) real_part_size.size(); i++)  {
		std::cout << "part " << std::setw(2) << i << " : "<< real_part_size[i] << "\n";
	}

	exit(0);
}

void TaskManager::merge_partition(int64_t partition_id) {



}


void TaskManager::merge_from_input(int64_t offset, int64_t len) {

	if (len == 0)
		return;

	/* Read the input file from [offset] to [offset + len] */
	make_read_input_task(this, offset, len);
}




void TaskManager::merge_from_output_run(std::vector<Run*> run_vector) {

	for (std::vector<Run*>::iterator it = run_vector.begin(); it != run_vector.end(); it++) {
		insert_run(*it);
	}

	/* Insert merge sort task */
	make_merge_sort_task(this);
}




void TaskManager::insert_run(Run* run) {
	/* Insert a run */
	{
		std::unique_lock<std::mutex> lock(run_list_lock);

		/* Update total length about runs in run_list */
		len_in_run_list += run->len;

		std::list<Run*>::iterator it;
		for (it = run_list.begin(); it != run_list.end(); it++) {
			if (run->chunk_num <= (*it)->chunk_num) {
				run_list.insert(it, run);
				return;
			}
		}
		this->run_list.insert(it, run);
	}
}


std::vector<Run*>* TaskManager::get_run_vector() {
	{
		std::unique_lock<std::mutex> lock(run_list_lock);

		if (run_list.size() < (uint64_t) merge_way_num && len_in_run_list != output_run_len) {
			return NULL;
		} else {
			/* Return run vector */
			std::vector<Run*>* ret = new std::vector<Run*>();
			ret->reserve(merge_way_num);
			for (int64_t i = 0; i < merge_way_num; i++) {
				if (run_list.empty() == true) break;
				ret->emplace_back(run_list.front());
				len_in_run_list -= run_list.front()->len;
				run_list.pop_front();
			}
			return ret;
		}
	}
}











