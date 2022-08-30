# Compiler and Compile options.


# Macros specifying path for compile.

# Macros for workloads.
INPUT = input
OUTPUT = output
EXLARGE = ./workloads/exlarge/
SMALL = ./workloads/small/
MEDIUM = ./workloads/medium/
LARGE = ./workloads/large/





RUN = ./run_sort.sh




# Run command.

all: clean
	$(MAKE) -C ./hyunsoo_large --silent
	$(MAKE) -C ./kihwang_small --silent
	sleep 2

# Delete binary & object files
clean:
	$(MAKE) -C ./hyunsoo_large clean --silent
	$(MAKE) -C ./kihwang_small clean --silent
	$(RM) submission.tar.gz

# Ex-large (100G)
gen_exlarge:
	mkdir -p $(EXLARGE)
	./workloads/gensort 1000000000 $(EXLARGE)$(INPUT)

val_exlarge:
	./workloads/valsort $(EXLARGE)$(OUTPUT)

run_exlarge: all
	$(RUN) $(EXLARGE)$(INPUT) $(EXLARGE)$(OUTPUT)

rm_exlarge:
	$(RM) $(EXLARGE)$(OUTPUT)

test_exlarge: run_exlarge val_exlarge


# Small (10G)
gen_small:
	mkdir -p $(SMALL)
	./workloads/gensort 100000000 $(SMALL)$(INPUT)

val_small:
	./workloads/valsort $(SMALL)$(OUTPUT)

run_small: all
	$(RUN) $(SMALL)$(INPUT) $(SMALL)$(OUTPUT)

rm_small:
	$(RM) $(SMALL)$(OUTPUT)

test_small: run_small val_small


# Medium (20G)
gen_medium:
	mkdir -p $(MEDIUM)
	./workloads/gensort -a 200000000 $(MEDIUM)$(INPUT)

val_medium:
	./workloads/valsort $(MEDIUM)$(OUTPUT)

run_medium: all
	$(RUN) $(MEDIUM)$(INPUT) $(MEDIUM)$(OUTPUT)

rm_medium:
	$(RM) $(MEDIUM)$(OUTPUT)

test_medium: run_medium val_medium


# Large (60G)
gen_large:
	mkdir -p $(LARGE)
	./workloads/gensort -s 600000000 $(LARGE)$(INPUT)

val_large:
	./workloads/valsort $(LARGE)$(OUTPUT)

run_large: all
	$(RUN) $(LARGE)$(INPUT) $(LARGE)$(OUTPUT)

rm_large:
	$(RM) $(LARGE)$(OUTPUT)

test_large: run_large val_large


