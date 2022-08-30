#pragma once



#include <math.h>
//#define VERBOSE


#define TUPLESIZE	(100)
#define KEYSIZE		(10)


/* CHUNK_SIZE * CHUNK_NUM <= tatal memory size */

#define CHUNK_SIZE	(4000LL * 512 * 5) /* 10M */
#define CHUNK_NUM	(17 * 1)
#define READ_IO_SIZE	(4000LL * 512 * 5) /* 10M */
#define WRITE_IO_SIZE	(4000LL * 512 * 5) /* 10M */
#define WORKER_THREAD_NUM	(38)
#define IO_THREAD_NUM	(17)
#define MERGE_WAY_NUM	(36)

/* Read SUB_RUN_SIZE, Sort merge, Flush PART_FILE_SIZE */
#define PART_FILE_SIZE	(1000LL * 1000 * 1300 * 1) /* 1.3G */
#define SUB_RUN_SIZE	(1000LL * 1000 * 1000 * 2) /* 2G */

/* For histogram */
#define HIST_PREFIX_NUM	(16)
#define HIST_PART_NUM	(10)





/* Utils */


#define mb() __sync_synchronize()


extern void print_time(const char str[]);

extern void print_info();

extern int workload_type;
extern int64_t key2pid(char* key);






