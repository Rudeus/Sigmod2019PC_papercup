# Compiler and Compile options.
CC = g++
CPPFLAGS = -g -Wall -std=c++17 -lpthread


# Optimize
OPT += -O2 -mavx -msse2
#OPT += -O0

# Macros in code
#OPT += -DVERBOSE


#OPT += -DLOCAL




# Macros specifying path for compile.
SRCS := $(wildcard src/*.cc)
OBJS := $(SRCS:.cc=.o)
BIN = ./build/release/
INC = ./include/
LIB = ./lib/ -lpthread

# Macros for workloads.
INPUT = input
OUTPUT = output
TINY = ../workloads/tiny/
SMALL = ../workloads/small/
MEDIUM = ../workloads/medium/
LARGE = ../workloads/large/

# Pre-Processor.
CPPFLAGS += $(OPT)
CPPFLAGS += -I$(INC)

# Compile command.
TARGET = sort


$(TARGET): clean $(OBJS)
	mkdir -p $(BIN)
	$(CC) $(CFLAGS) -o $(BIN)$(TARGET) $(OBJS) -L$(LIB)



# Run command.

perf_report:
	sudo ~/perf report --children -g graph -s sym

# Tiny (1G)
gen_tiny:
	mkdir -p $(TINY)
	../workloads/gensort 10000000 $(TINY)$(INPUT)

gen_tiny_a:
	mkdir -p $(TINY)
	../workloads/gensort -a 10000000 $(TINY)$(INPUT)

val_tiny:
	../workloads/valsort $(TINY)$(OUTPUT)

run_tiny: $(TARGET)
	$(BIN)$(TARGET) $(TINY)$(INPUT) $(TINY)$(OUTPUT)

rm_tiny:
	$(RM) $(TINY)$(OUTPUT)

test_tiny: run_tiny val_tiny

perf_tiny: $(TARGET)
	sudo ~/perf record -g $(BIN)$(TARGET) $(TINY)$(INPUT) $(TINY)$(OUTPUT)


# Small (10G)
gen_small:
	mkdir -p $(SMALL)
	../workloads/gensort 100000000 $(SMALL)$(INPUT)

gen_small_a:
	mkdir -p $(SMALL)
	../workloads/gensort -a 100000000 $(SMALL)$(INPUT)

val_small:
	../workloads/valsort $(SMALL)$(OUTPUT)

run_small: $(TARGET)
	$(BIN)$(TARGET) $(SMALL)$(INPUT) $(SMALL)$(OUTPUT)

rm_small:
	$(RM) $(SMALL)$(OUTPUT)

test_small: run_small val_small

perf_small: $(TARGET)
	sudo ~/perf record -g $(BIN)$(TARGET) $(SMALL)$(INPUT) $(SMALL)$(OUTPUT)


# Medium (20G)
gen_medium:
	mkdir -p $(MEDIUM)
	../workloads/gensort 200000000 $(MEDIUM)$(INPUT)

gen_medium_a:
	mkdir -p $(MEDIUM)
	../workloads/gensort -a 200000000 $(MEDIUM)$(INPUT)

val_medium:
	../workloads/valsort $(MEDIUM)$(OUTPUT)

run_medium: $(TARGET)
	$(BIN)$(TARGET) $(MEDIUM)$(INPUT) $(MEDIUM)$(OUTPUT)

rm_medium:
	$(RM) $(MEDIUM)$(OUTPUT)

test_medium: run_medium val_medium

perf_medium: $(TARGET)
	sudo ~/perf record -g $(BIN)$(TARGET) $(MEDIUM)$(INPUT) $(MEDIUM)$(OUTPUT)


# Large (60G)
gen_large:
	mkdir -p $(LARGE)
	../workloads/gensort 600000000 $(LARGE)$(INPUT)

gen_large_a:
	mkdir -p $(LARGE)
	../workloads/gensort -a 600000000 $(LARGE)$(INPUT)

val_large:
	../workloads/valsort $(LARGE)$(OUTPUT)

run_large: $(TARGET)
	$(BIN)$(TARGET) $(LARGE)$(INPUT) $(LARGE)$(OUTPUT)

rm_large:
	$(RM) $(LARGE)$(OUTPUT)

test_large: run_large val_large

perf_large: $(TARGET)
	sudo ~/perf record -g $(BIN)$(TARGET) $(LARGE)$(INPUT) $(LARGE)$(OUTPUT)


# Delete binary & object files
clean:
	$(RM) $(BIN)$(TARGET) $(OBJS)
	$(RM) -r build
	$(RM) submission.tar.gz
