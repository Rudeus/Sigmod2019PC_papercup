#pragma once



#define LOCAL

#define HIST_THREE
//#define HIST_TWO

//#define VERBOSE
#define ATOM_CAS(dest, oldval, newval)\
	__sync_bool_compare_and_swap(&(dest), oldval, newval)
#define PAUSE { __asm__ ( "pause;" ); }
#define SMALL_SET (1000LL * 1000 * 1000 * 10)

#define TUPLESIZE	(100)
#define KEYSIZE		(10)

#define MEMSIZE	(1024LL*1024*1024*30) // 30GiB
#define EXPECT_PARTITION (1000LL * 1000 * 1000 * 12) // 5 G
#define INMEM_EXPECT_PARTITION (1000LL * 1000 * 100 * 25) // 2.5 G
#define HIST_CHUNK_SIZE	(1000LL * 100 * 1000)	// 100M
#define HIST_CHUNK_NUM	(40)
#define INMEM_HIST_CHUNK_NUM (5)
#ifdef HIST_THREE
#define HIST_RANGE	(256 * 256 * 256) // 3-byte range
#define INMEM_HIST_RANGE (256) // 1-byte range
#endif
#ifdef HIST_TWO
#define HIST_RANGE	(256 * 256) // 2-byte range
#define INMEM_HIST_RANGE (256) // 1-byte range
#endif

/* CHUNK_SIZE * CHUNK_NUM <= total memory size */
#ifdef LOCAL
#define CHUNK_SIZE	(1000LL * 100 * 100) /* 10M */
#define CHUNK_NUM	(3220) //3220
#define WORKER_THREAD_NUM	(38)
#define READ_THREAD_NUM	(20)
#define MERGE_WAY_NUM	(15)
#define INMEM_MERGE_WAY_NUM (10)
#else
#define CHUNK_SIZE	(2000000000LL) /* 2G */
#define CHUNK_NUM	(10)
#define WORKER_THREAD_NUM	(20)
#define READ_THREAD_NUM	(10)
#define WAY_NUM		(20)
#endif

extern int PART_NUM;
extern int make_finish;
extern int* part_point;
extern int64_t* part_size;
extern int64_t* thread_part_size[READ_THREAD_NUM];
extern int64_t* sort_chunk_num;
extern int64_t* sort_end_num;
extern int* ext_flag;
extern int part_id;
extern int64_t hist[HIST_RANGE];
extern bool* last_merge;
extern uint64_t* g_write_counter;
extern uint64_t total_flush_size;
extern uint64_t total_file_size;
extern int inmemory_part_num;
extern int do_inmemory_part_num;
extern bool* overlap_flag;

/* Utils */

extern void print_time(const char str[]);

