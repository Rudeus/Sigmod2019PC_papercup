# Compiler and Compile options.
CC = g++
CPPFLAGS = -w -std=c++17 -lpthread
#CPPFLAGS = -g -Wall -std=c++17 -lpthread


# Optimize
OPT += -O2
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
EXLARGE = ../workloads/exlarge/
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


# Ex-large (100G)
gen_exlarge:
	mkdir -p $(EXLARGE)
	../workloads/gensort 1000000000 $(EXLARGE)$(INPUT)

val_exlarge:
	../workloads/valsort $(EXLARGE)$(OUTPUT)

run_exlarge: $(TARGET)
	$(BIN)$(TARGET) $(EXLARGE)$(INPUT) $(EXLARGE)$(OUTPUT)

rm_exlarge:
	$(RM) $(EXLARGE)$(OUTPUT)

test_exlarge: run_exlarge val_exlarge


# Small (10G)
gen_small:
	mkdir -p $(SMALL)
	../workloads/gensort 100000000 $(SMALL)$(INPUT)

val_small:
	../workloads/valsort $(SMALL)$(OUTPUT)

run_small: $(TARGET)
	$(BIN)$(TARGET) $(SMALL)$(INPUT) $(SMALL)$(OUTPUT)

rm_small:
	$(RM) $(SMALL)$(OUTPUT)

test_small: run_small val_small


# Medium (20G)
gen_medium:
	mkdir -p $(MEDIUM)
	../workloads/gensort -a 200000000 $(MEDIUM)$(INPUT)

val_medium:
	../workloads/valsort $(MEDIUM)$(OUTPUT)

run_medium: $(TARGET)
	$(BIN)$(TARGET) $(MEDIUM)$(INPUT) $(MEDIUM)$(OUTPUT)

rm_medium:
	$(RM) $(MEDIUM)$(OUTPUT)

test_medium: run_medium val_medium


# Large (60G)
gen_large:
	mkdir -p $(LARGE)
	../workloads/gensort -s 600000000 $(LARGE)$(INPUT)

val_large:
	../workloads/valsort $(LARGE)$(OUTPUT)

run_large: $(TARGET)
	$(BIN)$(TARGET) $(LARGE)$(INPUT) $(LARGE)$(OUTPUT)

rm_large:
	$(RM) $(LARGE)$(OUTPUT)

test_large: run_large val_large


# Delete binary & object files
clean:
	$(RM) $(BIN)$(TARGET) $(OBJS)
	$(RM) -r build
	$(RM) submission.tar.gz
