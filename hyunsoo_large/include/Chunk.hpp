#pragma once


#include <mutex>



#include <stdint.h>




/* Memory page. All data are buffered in this */
class Chunk {


	public:
	int64_t len;
	char* buffer;
	int64_t real_loc;
	int64_t temp_loc;
	int flag;

	std::mutex lock;


	Chunk();
	Chunk(int64_t size);
	~Chunk();

};












