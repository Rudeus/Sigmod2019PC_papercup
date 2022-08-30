#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>


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


#include <ThreadPool.hpp>
#include <ReadInput.hpp>
#include <WriteOutput.hpp>
#include <RunManager.hpp>
//#include <Run.hpp>



#include <Task.hpp>
int compare(const void* a, const void* b) {
	uint8_t* a_ptr;
	uint8_t* b_ptr;
	for (size_t i = 0; i < KEYSIZE; i += 1) {
		a_ptr = (uint8_t*) ((char*) a + i);
		b_ptr = (uint8_t*) ((char*) b + i);

		if (*a_ptr < *b_ptr) return -1;
		else if (*a_ptr > *b_ptr) return 1;
	}
	return 0;
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


//----------------------------------------------------------------
//----------------------------------------------------------------

void sample_task(int a, int b) {
	std::cout << a << " . " << b << "\n";
}

//----------------------------------------------------------------
//----------------------------------------------------------------

int64_t g_read_counter = 0;

void make_hist_task(int64_t location, Chunk* chunk) {
	int fd = read_input->fd;

	pread(fd, chunk->buffer, HIST_CHUNK_SIZE, location * HIST_CHUNK_SIZE);

	for (int64_t i = 0; i < HIST_CHUNK_SIZE; i += TUPLESIZE) {
#ifdef HIST_THREE
		uint64_t* loc_ptr1;
		uint64_t loc1;
		unsigned char loc2;
		if (inmemory_part_num != PART_NUM) {
			loc_ptr1 = (uint64_t*)&chunk->buffer[i];
			loc1 = __builtin_bswap64(*loc_ptr1);
			loc1 = loc1 >> 40;
			__sync_fetch_and_add(&hist[loc1], 1);
		}	else {
			loc2 = (unsigned char)chunk->buffer[i];
			__sync_fetch_and_add(&hist[loc2], 1);
		}
#endif
#ifdef HIST_TWO
		uint16_t* loc_ptr1;
		uint16_t loc1;
		unsigned char loc2;
		if (inmemory_part_num != PART_NUM) {
			loc_ptr1 = (uint16_t*)&chunk->buffer[i];
			loc1 = __builtin_bswap16(*loc_ptr1);
			__sync_fetch_and_add(&hist[loc1], 1);
		}	else {
			loc2 = (unsigned char)chunk->buffer[i];
			__sync_fetch_and_add(&hist[loc2], 1);
		}
#endif
	}
	__sync_fetch_and_add(&make_finish, 1);
}


void make_partition_task(int tid, int64_t iter_num, int64_t location, Chunk* chunk, int id) {
	int fd = read_input->fd;
	int sortnum[inmemory_part_num];
	bool all_inmemory = false;
	int64_t local_part_size[PART_NUM];
	Chunk* chunk_sort[inmemory_part_num];
	Chunk* chunk_partition[PART_NUM - inmemory_part_num];

	memset(sortnum, 0, sizeof(int) * inmemory_part_num);
	memset(local_part_size, 0, sizeof(int64_t) * PART_NUM);

	for (int i = 0; i < inmemory_part_num; i++)
		chunk_sort[i] = chunk_manager->get_free_chunk();
	for (int i = 0; i < PART_NUM - inmemory_part_num; i++)
		chunk_partition[i] = chunk_manager->get_free_chunk();

	for (int64_t j = 0; j < iter_num; j++) {

		pread(fd, chunk->buffer, HIST_CHUNK_SIZE, location + j * HIST_CHUNK_SIZE);

		for (int64_t i = 0; i < HIST_CHUNK_SIZE; i += TUPLESIZE) {
			int cont_flag = 0;
			int k;
#ifdef HIST_THREE
			uint64_t* loc_ptr1;
			uint64_t loc1;
			unsigned char loc2;
			int32_t val;
			if (inmemory_part_num != PART_NUM) {
				loc_ptr1 = (uint64_t*)&chunk->buffer[i];
				loc1 = __builtin_bswap64(*loc_ptr1);
				loc1 = loc1 >> 40;
				val = (int32_t)loc1;
			}	else {
				loc2 = (unsigned char)chunk->buffer[i];
				val = (int32_t)loc2;
			}
#endif
#ifdef HIST_TWO
			uint16_t* loc_ptr1;
			uint16_t loc1;
			unsigned char loc2;
			int16_t val;
			if (inmemory_part_num != PART_NUM) {
				loc_ptr1 = (uint16_t*)&chunk->buffer[i];
				loc1 = __builtin_bswap16(*loc_ptr1);
				val = loc1;
			}	else {
				loc2 = (unsigned char)chunk->buffer[i];
				val = (int16_t)loc2;
			}
#endif
			for (k = 0; k < inmemory_part_num; k++) {
				if (k == PART_NUM - 1) {
					all_inmemory = true;
					break;
				}
				if (val <= part_point[k]) {
					memcpy(chunk_sort[k]->buffer + chunk_sort[k]->len, chunk->buffer + i, 100);
					chunk_sort[k]->len += TUPLESIZE;

					if (chunk_sort[k]->len == CHUNK_SIZE) {
						Task* task = new Task();
						task_qsort_chunk_args* args = (task_qsort_chunk_args*) malloc(sizeof(task_qsort_chunk_args));
						args->chunk = chunk_sort[k];
						args->id = k;
						task->make_task(TASK_QSORT_CHUNK, args);
						thread_pool->insert_task(task);

						sortnum[k]++;
						local_part_size[k] += chunk_sort[k]->len;
						chunk_sort[k] = chunk_manager->get_free_chunk();
					}
					cont_flag = 1;
					break;
				}
			}

			if (all_inmemory && k == PART_NUM - 1) {
				memcpy(chunk_sort[k]->buffer + chunk_sort[k]->len, chunk->buffer + i, 100);
				chunk_sort[k]->len += TUPLESIZE;

				if (chunk_sort[k]->len == CHUNK_SIZE) {
					Task* task = new Task();
					task_qsort_chunk_args* args = (task_qsort_chunk_args*) malloc(sizeof(task_qsort_chunk_args));
					args->chunk = chunk_sort[k];
					args->id = k;
					task->make_task(TASK_QSORT_CHUNK, args);
					thread_pool->insert_task(task);

					sortnum[k]++;
					local_part_size[k] += chunk_sort[k]->len;
					chunk_sort[k] = chunk_manager->get_free_chunk();
				}
				cont_flag = 1;
			}

			if (cont_flag) continue;

			for (k = inmemory_part_num; k < PART_NUM - 1; k++) {
				if (val <= part_point[k]) {  // file name is part_#threadid_0
					memcpy(chunk_partition[k - inmemory_part_num]->buffer + chunk_partition[k - inmemory_part_num]->len, chunk->buffer + i, TUPLESIZE);
					chunk_partition[k - inmemory_part_num]->len += TUPLESIZE;

					if (chunk_partition[k - inmemory_part_num]->len == CHUNK_SIZE) {
						local_part_size[k] += chunk_partition[k - inmemory_part_num]->len;
						thread_part_size[tid][k - inmemory_part_num] += chunk_partition[k - inmemory_part_num]->len;
						write(write_output->part_fd[tid][k - inmemory_part_num], chunk_partition[k - inmemory_part_num]->buffer, CHUNK_SIZE);
						chunk_partition[k - inmemory_part_num]->len = 0;
					}
					break;
				}
			}

			if (k == PART_NUM - 1) { // file name is part_#threadid_1
				memcpy(chunk_partition[k - inmemory_part_num]->buffer + chunk_partition[k - inmemory_part_num]->len, chunk->buffer + i, TUPLESIZE);
				chunk_partition[k - inmemory_part_num]->len += TUPLESIZE;

				if (chunk_partition[k - inmemory_part_num]->len == CHUNK_SIZE) {
					local_part_size[k] += chunk_partition[k - inmemory_part_num]->len;
					thread_part_size[tid][k - inmemory_part_num] += chunk_partition[k - inmemory_part_num]->len;
					write(write_output->part_fd[tid][k - inmemory_part_num], chunk_partition[k - inmemory_part_num]->buffer, CHUNK_SIZE);
					chunk_partition[k - inmemory_part_num]->len = 0;
				}
			}
		}
	}

	// last write
	for (int i = inmemory_part_num; i < PART_NUM; i++) {
		local_part_size[i] += chunk_partition[i - inmemory_part_num]->len;
		thread_part_size[tid][i - inmemory_part_num] += chunk_partition[i - inmemory_part_num]->len;
		write(write_output->part_fd[tid][i - inmemory_part_num], chunk_partition[i - inmemory_part_num]->buffer, chunk_partition[i - inmemory_part_num]->len);
		chunk_manager->free_chunk(chunk_partition[i - inmemory_part_num]);
	}

	// last sort task
	for (int i = 0; i < inmemory_part_num; i++) {
		Task* task = new Task();
		task_qsort_chunk_args* args = (task_qsort_chunk_args*) malloc(sizeof(task_qsort_chunk_args));
		args->chunk = chunk_sort[i];
		args->id = i;
		task->make_task(TASK_QSORT_CHUNK, args);
		thread_pool->insert_task(task);
		sortnum[i]++;
		local_part_size[i] += chunk_sort[i]->len;
		__sync_fetch_and_add(&sort_chunk_num[i], sortnum[i]);
	}

	for (int i = 0; i < PART_NUM; i++)
		__sync_fetch_and_add(&part_size[i], local_part_size[i]);

	if (!all_inmemory) {
		for (int i = 0; i < inmemory_part_num; i++)
			__sync_fetch_and_add(&ext_flag[i], 1);
	} else {
		for (int i = 0; i < inmemory_part_num; i++)
			__sync_fetch_and_add(&sort_end_num[i], 1);
	}

	if (ext_flag[id] == READ_THREAD_NUM + 1 || (all_inmemory && sort_end_num[id] == READ_THREAD_NUM)) {
		for(int i = 0; i < PART_NUM; i++) {
			total_file_size += part_size[i];
			printf("part%d: %lld\n", i, part_size[i]);
		}
		printf("total file size: %lld\n", total_file_size);
	}
	// if ext_flag == read thread num + 1 --> every read threads' classification is done
}

void partition_sort_task(int tid, int id) {
	int fd = write_output->part_fd[tid][id - inmemory_part_num];
	int sortnum = 0;
	Chunk* chunk_sort;
	int iter_num;
	//printf("partition[%d][%d] size: %lld\n", tid, id - 1, size);

	int64_t size = thread_part_size[tid][id - inmemory_part_num];
	iter_num = size / CHUNK_SIZE;
	if (size % CHUNK_SIZE != 0) iter_num++;
	chunk_sort = chunk_manager->get_free_chunk();


	//printf("partition sort task start\n");

	for (int64_t j = 0; j < iter_num; j++) {

		int64_t len;
		if (j == iter_num - 1) len = pread(fd, chunk_sort->buffer, size - j * CHUNK_SIZE, j * CHUNK_SIZE);
		else len = pread(fd, chunk_sort->buffer, CHUNK_SIZE, j * CHUNK_SIZE);
		chunk_sort->len = len;
		Task* task = new Task();
		task_qsort_chunk_args* args = (task_qsort_chunk_args*) malloc(sizeof(task_qsort_chunk_args));
		args->chunk = chunk_sort;
		args->id = id;
		task->make_task(TASK_QSORT_CHUNK, args);
		thread_pool->insert_task(task);

		sortnum++;
		chunk_sort = chunk_manager->get_free_chunk();
	}

	__sync_fetch_and_add(&sort_chunk_num[id], sortnum);
	__sync_fetch_and_add(&ext_flag[id], 1);
	//printf("partition sort task end\n");
}

void read_input_task(int64_t offset, int64_t len, int id) {

	assert(offset + len <= read_input->file_size);

	for (int64_t i = 0; i < len; i += CHUNK_SIZE) {
		int64_t chunk_len = CHUNK_SIZE;

		/* Read */
		Chunk* chunk = read_input->read_chunk(offset + i, chunk_len);

		/* Insert a quick sort task */
		Task* task = new Task();
		task_qsort_chunk_args* args = (task_qsort_chunk_args*) malloc(sizeof(task_qsort_chunk_args));
		args->chunk = chunk;
		args->id = id;
		task->make_task(TASK_QSORT_CHUNK, args);
		thread_pool->insert_task(task);
	}

	int64_t counter = __sync_add_and_fetch(&g_read_counter, len);
	if (counter == read_input->file_size) {
		print_time("local read is done");
	}
}

//----------------------------------------------------------------
// TT

/**
	Set sibling node.
 **/
void T_Tree::set_sibling(std::vector<T_Node*>& node_list) {
	for (int i = 0; i < node_list.size();) {
		if (i + 1 == node_list.size()) {
			assert(node_list[i] -> c_ptr == nullptr);
			break;
		} else {
			node_list[i]->c_ptr = node_list[i+1];
			node_list[i+1]->c_ptr = node_list[i];
		}
		i += 2;
	}
}

/**
	Set parent node.
 **/
void T_Tree::set_parent(std::vector<T_Node*>& child_list, std::vector<T_Node*>& parent_list) {

	uint32_t child_index = 0;

	for (int i = 0; i < parent_list.size(); ++i) {

		if (child_index + 1 == child_list.size()) {
			child_list[child_index++]->p_ptr = parent_list[i];

			assert(i == parent_list.size()-1);

		} else {
			child_list[child_index++]->p_ptr = parent_list[i];
			child_list[child_index++]->p_ptr = parent_list[i];
		}
	}

	assert(child_index == child_list.size());
}
void T_Tree::build_init_tree() {

	// Make leaf Nodes
	std::vector<T_Node*> C_node_list;
	std::vector<T_Node*> P_node_list;

	for (auto i = 0; i < todo_chunks; ++i) {
		T_Node* leaf_node = make_T_node();
		leaf_ptr.push_back(leaf_node);
		link_node.push_back(leaf_node);
	}

	C_node_list = leaf_ptr;

	uint32_t node_size = todo_chunks;
	uint32_t new_node_size = 0;

	while (1) {

		new_node_size = node_size / 2 + node_size % 2;

		// Make inner node first.
		for (int i = 0 ; i < new_node_size; ++i) {
			T_Node* inter_node = make_T_node();
			P_node_list.push_back(inter_node);
			link_node.push_back(inter_node);
		}

		set_sibling(C_node_list);
		set_parent(C_node_list, P_node_list);

		if (new_node_size == 1) {
			// Make it root & break.
			this->root_ptr = P_node_list[0];
			break;
		}

		C_node_list.clear();
		C_node_list = P_node_list;
		P_node_list.clear();
		node_size = new_node_size;
	}
}

T_Node* T_Tree::make_T_node() {
	T_Node * temp = new T_Node();
	return temp;
}


void T_Tree::push(struct pair pair) {
	leaf_ptr[leaf_index++]->set_pair(pair);
}

// Merge function doesn't call when num of leaf node is only one.
void T_Tree::init_tree() {

	std::vector<T_Node*> T_node_list = leaf_ptr;
	std::vector<T_Node*> tmp;

	uint32_t node_index;
	uint32_t node_num;

	T_Node* parent_ptr = nullptr;
	while (1) {
		node_index = 0;
		node_num = T_node_list.size();

		if (node_num == 1)
			break;

		while(1) {
			parent_ptr = T_node_list[node_index]->p_ptr;
			tmp.push_back(parent_ptr);
			if (T_node_list[node_index]->c_ptr == nullptr) { // Only remain one node. set Parent's pair to current node's pair.
				parent_ptr->set_pair(T_node_list[node_index]->pair);
				break;
			} else {
				if (compare(T_node_list[node_index]->pair.ptr, T_node_list[node_index+1]->pair.ptr) >= 0) {
					parent_ptr->set_pair(T_node_list[node_index+1]->pair);
				} else {
					parent_ptr->set_pair(T_node_list[node_index]->pair);
				}
				node_index += 2;
			}
			if (node_index == T_node_list.size())
				break;
		}
		T_node_list.clear();
		T_node_list = tmp;
		tmp.clear();
	}
}

void T_Tree::read_next_pair(uint64_t num) {
	struct pair pair;
	pair.num = num;
	pair.ptr = run_vector[num]->read_next();
	// This Run is over.
	if (pair.ptr == nullptr) {
		leaf_ptr[num]->active = false;
		// This Run is still running.
	} else {
		leaf_ptr[num]->set_pair(pair);
	}

	T_Node* cur = leaf_ptr[num];
	T_Node* comp = leaf_ptr[num]->c_ptr;

	while (1) {
		if (cur == root_ptr){
			break;
		}
		// Sibling node.
		comp = cur->c_ptr;

		// If there is no comp node.
		if (!comp) {
			if (cur -> active){
				cur->p_ptr->set_pair(cur->pair);
			} else {
				cur->p_ptr->active = false;
			}
			cur = cur->p_ptr;
			// Other case, Must check whether active or not.
		} else {
			if (cur->active && comp->active) {
				if (compare(cur->pair.ptr, comp->pair.ptr) < 0){
					cur->p_ptr->set_pair(cur->pair);
				} else {
					cur->p_ptr->set_pair(comp->pair);
				}
			} else if (cur->active && !comp->active) {
				cur->p_ptr->set_pair(cur->pair);
			} else if (!cur->active && comp->active) {
				cur->p_ptr->set_pair(comp->pair);
			} else {
				cur->p_ptr->active = false;
			}
			cur = cur->p_ptr;
		}
	}
}

// Pop thing.
struct pair T_Tree::pop() {
	struct pair ret = this->root_ptr->pair; // Find the min pair.
	//read_next_pair(ret.num);
	return ret;
}


T_Tree::~T_Tree() {
	// Free the tree
	// Free All tree.
	for (auto i = 0; i < link_node.size(); ++i)
		free(link_node[i]);
}


//----------------------------------------------------------------

int compare_memcmp(const void* a, const void* b) {

	return memcmp(a, b, KEYSIZE);
}

/* Quick sort it */
void qsort_chunk_task(Chunk* chunk, int id) {
#ifdef VERBOSE
	//std::cout << "task : qsort -> " << chunk->real_loc << "\n";
#endif
	assert(chunk != NULL);

	if (chunk->len == 0) {
		chunk_manager->free_chunk(chunk);
		return;
	}

	char* buffer = chunk->buffer;
	int64_t size = chunk->len;



	/* Do quick sort */
	//if (ext_flag[id])
	qsort(buffer, size / TUPLESIZE, TUPLESIZE, compare);
	//else qsort(buffer, size / TUPLESIZE, TUPLESIZE, compare_64);

	/* Make run */
	Run* run = new Run(chunk);

	/* This run is ready to merge */
	run_managers[id]->insert_run(run);

	if (ext_flag[id]) {
		__sync_fetch_and_add(&sort_end_num[id], 1);
		if (ext_flag[id] == READ_THREAD_NUM + 1 && sort_end_num[id] == sort_chunk_num[id]) {
			print_time("all chunk qsort end");
		}
	}

	/* Insert merge task */
	Task* task = new Task();
	task_multi_way_merge_sort_args* args = (task_multi_way_merge_sort_args*)malloc(sizeof(task_multi_way_merge_sort_args));
	args->id = id;
	task->make_task(TASK_MULTI_WAY_MERGE_SORT, args);
	thread_pool->insert_task(task);

}



//----------------------------------------------------------------
//----------------------------------------------------------------



struct compare_pair {
	bool operator() (struct pair a, struct pair b) {
		//if (ext_flag[part_id])
		return compare(a.ptr, b.ptr) > 0;
		//else return compare_64(a.ptr, b.ptr) > 0;
	}
};


void multi_way_merge_sort_task(int id) {

	std::list<Run*>* run_list;

	/* Get run list */
	if (inmemory_part_num == PART_NUM)
		run_list = run_managers[id]->get_run_list(INMEM_MERGE_WAY_NUM, id);
	else
		run_list = run_managers[id]->get_run_list(MERGE_WAY_NUM, id);

	if (run_list == NULL) {
		return;
	}

	/* Make run vector */
	std::vector<Run*> run_vector;
	for (std::list<Run*>::iterator iter = run_list->begin(); iter != run_list->end(); iter++) {
		run_vector.emplace_back(*iter);
	}

	// Make Tree.
	T_Tree* t_tree = new T_Tree(run_vector.size(), run_vector);
	t_tree->build_init_tree();
	// Make Heap
	//std::vector<pair> heap;
	int64_t len = 0;
	int64_t chunk_num = 0;
	for (uint64_t i = 0; i < run_vector.size(); i++) {
		struct pair pair;
		pair.num = i;
		pair.ptr = run_vector[i]->read_next();
		// heap
		//heap.push_back(pair);
		// T_TREE
		t_tree->push(pair);
		assert(pair.ptr != NULL);
		len += run_vector[i]->len;
		chunk_num += run_vector[i]->chunk_num;
	}

	/* Make heap */
	//std::make_heap(heap.begin(), heap.end(), compare_pair());
	/* Make tree */
	t_tree->init_tree();
	/* Output run */
	Run* output_run = new Run(chunk_num, len);

#ifdef VERBOSE
	std::cout << "task : multi merge -> ";
	for (uint64_t i = 0; i < run_vector.size(); i++) {
		std::cout << run_vector[i]->chunk_num << ' ';
	}
	std::cout << "-> " << output_run->chunk_num << "\n";
#endif

	if (ext_flag[id] == READ_THREAD_NUM + 1) {
		if (output_run->len == part_size[id]) {
			last_merge[id] = true;
			//print_time("start last merge");
		}
	} else if (sort_end_num[id] == READ_THREAD_NUM && output_run->len == part_size[id] && inmemory_part_num == PART_NUM) {
		/* Last merge, write output file */
		last_merge[id] = true;
		//printf("start last merge id: %d\n", id);
	}

	// ----- TT -----
	for (;;) {
		if (!t_tree->root_ptr->active)
			break;
		struct pair pair = t_tree->pop();
		// Write tuple 
		if (last_merge[id] == true) {
			// Write to disk 
			output_run->full_flush_next(pair.ptr, id);
		} else {
			// Write to mem 
			output_run->write_next(pair.ptr);
		}
		t_tree->read_next_pair(pair.num);
	}

	// ----- TT -----

	/**for (;;) {
		if (heap.size() == 0) {
		break;
		}

	// Get min 
	struct pair pair = heap.front();
	assert(pair.ptr != NULL);

	// Write tuple 
	if (last_merge[id] == true) {
	// Write to disk 
	output_run->full_flush_next(pair.ptr, id);
	} else {
	// Write to mem 
	output_run->write_next(pair.ptr);
	}

	// Pop 
	std::pop_heap(heap.begin(), heap.end(), compare_pair());
	heap.pop_back();

	pair.ptr = run_vector[pair.num]->read_next();
	if (pair.ptr == NULL) {
	continue;
	}

	// Push 
	heap.push_back(pair);
	std::push_heap(heap.begin(), heap.end(), compare_pair());
	}**/

	if (last_merge[id] == false) {
		/* insert in run list */
		run_managers[id]->insert_run(output_run);

		/* Insert merge task */
		Task* task = new Task();
		task_multi_way_merge_sort_args* args = (task_multi_way_merge_sort_args*)malloc(sizeof(task_multi_way_merge_sort_args));
		args->id = id;
		task->make_task(TASK_MULTI_WAY_MERGE_SORT, args);
		thread_pool->insert_task(task);
	}
	delete t_tree;
	//free(t_tree);
}

//----------------------------------------------------------------
//----------------------------------------------------------------

/* Flush a chunk to output file */
void flush_real_task(Chunk* chunk, int id) {
#ifdef VERBOSE
	std::cout << "task : flush output file -> " << chunk->real_loc << "\n";
#endif
	if (ext_flag[id]) {
		int64_t chunk_loc = chunk->real_loc;

		/*if (chunk_loc > chunk_manager->max_chunk_id) {
		// Overflow
		return;
		}*/

		/* Write this chunk to the output file */
		write_output->write_chunk(chunk, id);

		uint64_t ret = __sync_add_and_fetch(&g_write_counter[id], chunk->len);

		{
			//while(!ATOM_CAS(chunk_manager->blatch, false, true))
			//	PAUSE;
			std::unique_lock<std::mutex> lock(chunk_manager->free_chunks_lock);

			chunk->len = 0;
			chunk_manager->free_chunks.emplace_front(chunk);
			//chunk_manager->blatch = false;
		}

		// overlap
		if (ret >= part_size[id] / 4 * 3 && overlap_flag[id]) {
			__sync_fetch_and_sub(&do_inmemory_part_num, 1);
			overlap_flag[id] = false;
		}

		if (ret == part_size[id]) {
			printf("partition real flush end // id: %d\n", id);
			__sync_fetch_and_add(&total_flush_size, g_write_counter[id]);
			//__sync_fetch_and_sub(&do_inmemory_part_num, 1);
		}

	} else {
		int64_t chunk_loc = chunk->real_loc;

		/* Write this chunk to the output file */
		write_output->write_chunk(chunk, id);

		uint64_t val = __sync_add_and_fetch(&g_write_counter[id], chunk->len);

		{
			//while(!ATOM_CAS(chunk_manager->blatch, false, true))
			//	PAUSE;
			std::unique_lock<std::mutex> lock(chunk_manager->free_chunks_lock);

			chunk_manager->free_chunks.emplace_front(chunk);
			//chunk_manager->blatch = false;
		}

		if (val == part_size[id]) {
			printf("partition real flush end // id: %d\n", id);
			__sync_fetch_and_add(&do_inmemory_part_num, 1);
		}

		/*if (g_write_counter[part_id] == read_input->file_size) {
			free(part_size);
			free(part_point);
			free(g_write_counter);
			free(sort_chunk_num);
			free(sort_end_num);
			free(ext_flag);
			free(last_merge);
			write_output->end();
			print_time("end");
			std::cout << "\n";
			exit(0);
			}*/
	}
}

//----------------------------------------------------------------
//----------------------------------------------------------------

Task::Task() {

}

Task::~Task() {

}


void Task::make_task(int task, void* args) {

	this->task = task;
	this->args = args;

}


void Task::run() {

	if (this->task == TASK_SAMPLE) {
		struct task_sample_args* args = (struct task_sample_args*) this->args;
		sample_task(args->a, args->b);
		free(args);
	} else if (this->task == TASK_MAKE_HIST) {
		struct task_make_hist_args* args = (struct task_make_hist_args*) this->args;
		make_hist_task(args->location, args->chunk);
		free(args);
	} else if (this->task == TASK_MAKE_PARTITION) {
		struct task_make_partition_args* args = (struct task_make_partition_args*) this->args;
		make_partition_task(args->tid, args->iter_num, args->location, args->chunk, args->id);
		free(args);
	}	else if (this->task == TASK_PARTITION_SORT) {
		struct task_partition_sort_args* args = (struct task_partition_sort_args*) this->args;
		partition_sort_task(args->tid, args->id);
		free(args);
	}	else if (this->task == TASK_READ_INPUT) {
		struct task_read_input_args* args = (struct task_read_input_args*) this->args;
		read_input_task(args->offset, args->len, args->id);
		free(args);
	} else if (this->task == TASK_QSORT_CHUNK) {
		struct task_qsort_chunk_args* args = (struct task_qsort_chunk_args*) this->args;
		qsort_chunk_task(args->chunk, args->id);
		free(args);
	} else if (this->task == TASK_MULTI_WAY_MERGE_SORT) {
		struct task_multi_way_merge_sort_args* args = (struct task_multi_way_merge_sort_args*) this->args;
		multi_way_merge_sort_task(args->id);
		free(args);
	} else if (this->task == TASK_FLUSH_REAL) {
		struct task_flush_real_args* args = (struct task_flush_real_args*) this->args;
		flush_real_task(args->chunk, args->id);
		free(args);
	}
}




