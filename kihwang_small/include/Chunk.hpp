#pragma once


#include <mutex>



#include <stdint.h>




/* Memory page. All data are buffered in this */
class Chunk {


	public:
	int64_t len;
	bool done;
	char* buffer;


	public:
	Chunk();
	~Chunk();

	void write_next(char* ptr);
};












