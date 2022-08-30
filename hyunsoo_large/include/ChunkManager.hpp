#pragma once



#include <list>
#include <vector>
#include <mutex>


#include <stdint.h>


#include <Chunk.hpp>


class ChunkManager {

	public:
	int64_t file_size;
	int64_t max_chunk_id;
	int64_t chunk_num;
	//std::vector<Chunk*> chunks;
	std::list<Chunk*> free_chunks;


	/* For free_chunks list */
	std::mutex free_chunks_lock;
	bool blatch = false;

	int64_t additional_chunk = 0;
	
	


	public:
	ChunkManager(int64_t file_size);
	~ChunkManager();

	Chunk* get_free_chunk();
	void free_chunk(Chunk* chunk);
};


extern ChunkManager* chunk_manager;



