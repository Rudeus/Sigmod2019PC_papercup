

#include <vector>



#include <Chunk.hpp>
#include <ChunkManager.hpp>
#include <ReadInput.hpp>


#include <RunManager.hpp>




std::vector<RunManager*> run_managers;


RunManager::RunManager() {

}

RunManager::~RunManager() {

}


void RunManager::insert_run(Run* run) {
	{
		while(!ATOM_CAS(blatch, false, true))
			PAUSE;
		//std::unique_lock<std::mutex> lock(this->run_list_lock);

		std::list<Run*>::iterator it;
		for (it = this->run_list.begin(); it != this->run_list.end(); it++) {
			if (run->chunk_num <= (*it)->chunk_num) {
				this->run_list.insert(it, run);
				blatch = false;
				return;
			}
		}
		this->run_list.insert(it, run);
		blatch = false;
	}
}


std::list<Run*>* RunManager::get_run_list(int64_t num, int id) {

	std::list<Run*>* ret = new std::list<Run*>();
	{
		while(!ATOM_CAS(blatch, false, true))
			PAUSE;
		//std::unique_lock<std::mutex> lock(run_list_lock);

		if (run_list.size() < 2) {
			/* Only one run, skip */
			delete ret;
			ret = NULL;
		} else if (run_list.size() < (uint64_t) num) {
			/* If this is last merge, do it */
			int64_t sum = 0;
			std::list<Run*>::iterator iter = run_list.begin();
			if (ext_flag[id]) {
				for (int64_t i = 0; i < num; i++) {
					if (run_list.empty() == true) {
						break;
					}
					if (iter == run_list.end()) {
						break;
					}
					sum += (*iter)->len;
					iter++;
				}
				if (sum == part_size[id]) {
					for (int64_t i = 0; i < num; i++) {
						if (run_list.empty() == true) {
							break;
						}
						ret->emplace_front(run_list.front());
						run_list.pop_front();
					}
				} else {
					ret = NULL;
				}
			} else {
				for (int64_t i = 0; i < num; i++) {
					if (run_list.empty() == true) {
						break;
					}
					if (iter == run_list.end()) {
						break;
					}
					sum += (*iter)->len;
					iter++;
				}
				if (sum == part_size[id]) {
					for (int64_t i = 0; i < num; i++) {
						if (run_list.empty() == true) {
							break;
						}
						ret->emplace_front(run_list.front());
						run_list.pop_front();
					}
				} else {
					ret = NULL;
				}
			}
		} else {
			for (int64_t i = 0; i < num; i++) {
				if (run_list.empty() == true) {
					break;
				}
				ret->emplace_front(run_list.front());
				run_list.pop_front();
			}
		}
		blatch = false;
	}
	return ret;
}








