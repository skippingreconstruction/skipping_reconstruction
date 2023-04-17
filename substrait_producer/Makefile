.PHONY: all clean test

override CPPFLAGS += -I./
override CPPFLAGS += `pkg-config --cflags protobuf`
CXXFLAGS += -g -O2 -std=c++17

LDFLAGS += `pkg-config --libs-only-L protobuf`
LDLIBS += `pkg-config --libs-only-other protobuf`
LDLIBS += `pkg-config --libs-only-l protobuf`
LDLIBS += -pthread

EXECSUFFIX=

# CC=$(CXX)
# CC=g++-7
# CXX=g++-7
CC=g++
CXX=g++
CFLAGS=$(CXXFLAGS)

SUBSTRIAT_FILES = substrait/type.pb.o \
			substrait/type_expressions.pb.o \
			substrait/parameterized_types.pb.o \
			substrait/function.pb.o \
			substrait/extensions/extensions.pb.o \
			substrait/algebra.pb.o \
			substrait/capabilities.pb.o \
			substrait/plan.pb.o \
			substrait/partition.pb.o

COMMON_FILES = data_type/data_type.o \
			configuration.o \
			metadata/interval.o \
			metadata/boundary.o \
			metadata/complex_boundary.o \
			metadata/expression.o \
			metadata/schema.o \
			metadata/query.o \
			produce_plan/impl/build_substrait_impl_arrow.o \
			produce_plan/impl/build_substrait_impl_velox.o \
			produce_plan/build_substrait.o \
			produce_plan/helper.o

LATE_FILES = produce_plan/make_plan_aggregation.o \
					produce_plan/make_plan_join.o \
					produce_plan/join_sequence.o \
					produce_plan/produce_scan_parameter.o \
					produce_plan/produce_scan_parameter_aggregation.o \
					produce_plan/produce_scan_parameter_join.o

EARLY_FILES = baselines/produce_scan_parameter.o \
					baselines/make_plan_base.o \
					baselines/early_agg/make_plan.o \
					baselines/early_join/join_sequence.o \
					baselines/early_join/make_plan.o 

EARLY_PRODUCE_PARAMS  = baselines/produce_scan_parameter.o

PARTITIONER_FILES = partitioner/common.o \
					partitioner/model.o \
					partitioner/horizontal_partitioner.o \
					partitioner/hierarchical_partitioner.o

LATE_DRIVERS = engine/engine$(EXECSUFFIX)
EARLY_DRIVERS = baselines/engine$(EXECSUFFIX)
PARTITION_DRIVERS = partitioner/partitioner$(EXECSUFFIX)

TEST_DRIVERS = temp/temp$(EXECSUFFIX)

all: $(LATE_DRIVERS) $(EARLY_DRIVERS) $(PARTITION_DRIVERS)
test: $(TEST_DRIVERS)

clean:
	rm -f $(LATE_DRIVERS)
	rm -f $(EARLY_DRIVERS)
	rm -f $(PARTITION_DRIVERS)
	rm -f $(TEST_DRIVERS)
	rm -f $(COMMON_FILES)
	rm -f $(LATE_FILES)
	rm -f $(EARLY_FILES)
	rm -f $(PARTITIONER_FILES)
	rm -f $(SUBSTRIAT_FILES)

$(LATE_DRIVERS): $(SUBSTRIAT_FILES) $(COMMON_FILES) $(LATE_FILES)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) $(subst $(EXECSUFFIX),,$@.cpp) $^ $(LDLIBS) -o $@

$(EARLY_DRIVERS): $(SUBSTRIAT_FILES) $(COMMON_FILES) $(EARLY_FILES)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) $(subst $(EXECSUFFIX),,$@.cpp) $^ $(LDLIBS) -o $@

$(PARTITION_DRIVERS): $(SUBSTRIAT_FILES) $(COMMON_FILES) $(LATE_FILES) $(EARLY_PRODUCE_PARAMS) $(PARTITIONER_FILES)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) $(subst $(EXECSUFFIX),,$@.cpp) $^ $(LDLIBS) -o $@

$(TEST_DRIVERS): $(SUBSTRIAT_FILES) $(COMMON_FILES) $(LATE_FILES)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(LDFLAGS) $(subst $(EXECSUFFIX),,$@.cpp) $^ $(LDLIBS) -o $@
