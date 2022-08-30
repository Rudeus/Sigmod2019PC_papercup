
#include <iostream>

#include <stdint.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>

#include <Config.hpp>
#include <Chunk.hpp>
#include <Task.hpp>
#include <ThreadPool.hpp>


#include <ChunkManager.hpp>

ChunkManager* chunk_manager;


ChunkManager::ChunkManager(int64_t file_size) {
	this->file_size = file_size;
	chunk_num = CHUNK_NUM;
	
	chunks.reserve(chunk_num);
	for (int64_t i = 0; i < chunk_num; i++) {
		Chunk* chunk = new Chunk();
		chunks.emplace_back(chunk);
		free_chunks.emplace_front(chunk);
	}
}

ChunkManager::~ChunkManager() {

}



Chunk* ChunkManager::get_chunk() {
	Chunk* chunk;

	for (;;) {
		{
			std::unique_lock<std::mutex> lock(free_chunks_lock);

			if (free_chunks.size() == 0) {
				/* Empty */
				assert(free_chunks.size() != 0);
			} else {
				chunk = free_chunks.front();
				free_chunks.pop_front();
				break;
			}
		}
	}

	chunk->len = 0;
	return chunk;
}

void ChunkManager::free_chunk(Chunk* chunk) {

	{
		std::unique_lock<std::mutex> lock(free_chunks_lock);

		free_chunks.emplace_front(chunk);
	}
}


