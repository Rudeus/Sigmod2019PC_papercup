#pragma once

#include <Chunk.hpp>
#include <Run.hpp>
enum {
	TASK_SAMPLE,
	TASK_MAKE_HIST,
	TASK_MAKE_PARTITION,
	TASK_PARTITION_SORT,
	TASK_READ_INPUT,
	TASK_QSORT_CHUNK,
	TASK_MULTI_WAY_MERGE_SORT,
	TASK_FLUSH_REAL,
};

struct task_sample_args {
	int a;
	int b;
};

struct task_read_input_args {
	int64_t offset;
	int64_t len;
	int id;
};

struct task_make_hist_args {
	int64_t location;
	Chunk* chunk;
};

struct task_make_partition_args {
	int tid;
	int64_t iter_num;
	int64_t location;
	Chunk* chunk;
	int id;
};

struct task_partition_sort_args {
	int tid;
	int id;
};

struct task_qsort_chunk_args {
	Chunk* chunk;
	int id;
};

struct task_multi_way_merge_sort_args {
	int id;
};

struct task_flush_real_args {
	Chunk* chunk;
	int id;
};

struct pair {
	uint64_t num;
	char* ptr;
};

class Task {

	public:
		int task;
		void* args;


	public:
		Task();
		~Task();

		void make_task(int task, void* arg);
		void run();


};

class T_Node {

	public:
		struct pair pair;
		bool active;   // active or not.
		T_Node* p_ptr; // parent ptr.
		T_Node* c_ptr; // sibling ptr.

	public:
		T_Node(): p_ptr(nullptr), c_ptr(nullptr), active(true){};

		void set_pair(struct pair pair) {
			this->pair.num = pair.num;
			this->pair.ptr = pair.ptr;
		}

		T_Node(const T_Node& t) {
			p_ptr = t.p_ptr;
			c_ptr = t.c_ptr;
			active = t.active;
			pair = t.pair;
		}

		bool operator>(const T_Node& comp) const;
};


class T_Tree {
	public:
		uint32_t todo_chunks; // # of leaf nodes.

		std::vector<Run*> run_vector;
		T_Node* root_ptr;

		// [i] Leaf node connect to [i] Run.
		std::vector<T_Node*> leaf_ptr;
		std::vector<T_Node*> link_node;
		uint32_t leaf_index;

	public:
		T_Tree(uint32_t leaf_size, std::vector<Run*> &run_vector) : todo_chunks(leaf_size), run_vector(run_vector), leaf_index(0), root_ptr(nullptr){};
		// Leaf node size & Run vector.. To connect leaf node.
		~T_Tree();

		T_Node* make_T_node();

		void set_parent(std::vector<T_Node*>&, std::vector<T_Node*>&);
		void set_sibling(std::vector<T_Node*>&);

		void build_init_tree(); // called in constructor.
		void init_tree(); // Only call once.

		void read_next_pair(uint64_t);
		struct pair pop(); // Pop and get new pair in that run vector.

		void push(struct pair pair); // push called only once when begin.
};






