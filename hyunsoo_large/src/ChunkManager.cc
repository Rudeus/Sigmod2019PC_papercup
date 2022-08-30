
#include <iostream>

#include <assert.h>
#include <stdint.h>

#include <Config.hpp>
#include <Chunk.hpp>
#include <Task.hpp>
#include <ThreadPool.hpp>


#include <ChunkManager.hpp>

ChunkManager* chunk_manager;


ChunkManager::ChunkManager(int64_t file_size) {
	this->file_size = file_size;
	chunk_num = CHUNK_NUM;
	
	//chunks.reserve(chunk_num);
	for (int64_t i = 0; i < chunk_num; i++) {
		Chunk* chunk = new Chunk();
		chunk->len = 0;
		//chunks.emplace_back(chunk);
		free_chunks.emplace_front(chunk);
	}
}

ChunkManager::~ChunkManager() {

}



Chunk* ChunkManager::get_free_chunk() {
	Chunk* chunk;

	for (;;) {
		{
			//while(!ATOM_CAS(blatch, false, true))
			//	PAUSE;
			std::unique_lock<std::mutex> lock(free_chunks_lock);

			if (free_chunks.size() == 0) {
				/* Empty */
				//printf("no chunk\n");
				Chunk* chunk = new Chunk();
				chunk->len = 0;
				free_chunks.emplace_front(chunk);
				additional_chunk++;
				//assert(free_chunks.size() != 0);
				//continue;
			} else {
				chunk = free_chunks.front();
				free_chunks.pop_front();
				chunk->len = 0;
				//blatch = false;
				break;
			}
			//blatch = false;
		}
	}

	return chunk;
}

void ChunkManager::free_chunk(Chunk* chunk) {

	{
		//while(!ATOM_CAS(blatch, false, true))
		//	PAUSE;
		std::unique_lock<std::mutex> lock(free_chunks_lock);
		if (!additional_chunk) {
			chunk->len = 0;
			free_chunks.emplace_front(chunk);
		}
		else {
			delete chunk;
			additional_chunk--;
		}
		//blatch = false;
	}
}

