#pragma once


#include <list>
#include <mutex>
#include <thread>



#include <stdint.h>



#include <Run.hpp>




class RunManager {


	public:
	/* TODO: multi tier */
	std::list<Run*> run_list;

	std::mutex run_list_lock;
	bool blatch = false;



	public:
	RunManager();
	~RunManager();

	void insert_run(Run* run);
	std::list<Run*>* get_run_list(int64_t num, int id);

};



extern std::vector<RunManager*> run_managers;










