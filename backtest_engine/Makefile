CXX := g++
CXXFLAGS := -std=c++17 -O3 -Wall -Wextra -pedantic
TARGET := engine

all: $(TARGET)

$(TARGET): engine.cpp
	$(CXX) $(CXXFLAGS) engine.cpp -o $(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all clean
