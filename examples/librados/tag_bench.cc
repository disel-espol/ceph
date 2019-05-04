// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

// install the librados-dev package to get this
#include <rados/librados.hpp>
#include <boost/program_options.hpp>
#include <iostream>
#include <string>
#include <sstream>
#include <thread>
#include <random>
#include <cmath>
#include <hdr_histogram.h>
#include <chrono>
#include <atomic>
#include <vector>

std::atomic<int> read_ps(0);
std::atomic<int> write_ps(0);
std::atomic<int> is_done(0);

struct conf_t {
  librados::IoCtx& io_ctx;
  bool* write_map;
  std::string object_data;
  size_t data_size;
  double mean;
  double std_dev;
  double tag_dev;
  int64_t op_count;
  int64_t round_count;
  int64_t tag_count;
  struct hdr_histogram* read_histogram;
  struct hdr_histogram* write_histogram;
  bool exit_reporter;
};

static inline uint64_t __getns(clockid_t clock)
{
  struct timespec ts;
  clock_gettime(clock, &ts);
  return (((uint64_t)ts.tv_sec) * 1000000000ULL) + ts.tv_nsec;
}

static inline uint64_t getus()
{
  return __getns(CLOCK_MONOTONIC) / 1000;
}

class rand_data_gen {
 public:
  rand_data_gen(size_t buf_size, size_t samp_size) :
    buf_size_(buf_size),
    samp_size_(samp_size),
    dist_(0, buf_size_ - samp_size - 1)
  {}

  void generate() {
    std::uniform_int_distribution<uint64_t> d(
        std::numeric_limits<uint64_t>::min(),
        std::numeric_limits<uint64_t>::max());
    buf_.reserve(buf_size_);
    while (buf_.size() < buf_size_) {
      uint64_t val = d(gen_);
      buf_.append((const char *)&val, sizeof(val));
    }
    if (buf_.size() > buf_size_)
      buf_.resize(buf_size_);
  }

  inline const char *sample() {
    assert(!buf_.empty());
    // return buf_.c_str() + dist_(gen_);
    return buf_.c_str() + (buf_size_ - samp_size_ - 1);
  }

 private:
  const size_t buf_size_;
  const size_t samp_size_;
  std::string buf_;
  std::default_random_engine gen_;
  std::uniform_int_distribution<size_t> dist_;
};

void send_write_reqs(conf_t* conf){

  rand_data_gen dgen(1ULL << 25, 1024 * 1024 * 5);
  dgen.generate();
  std::string object_name("hello_object");
  unsigned long int size = 1024 * 1024 * 5;

  std::default_random_engine generator;
  generator.seed(std::chrono::system_clock::now().time_since_epoch().count());
  int loops = conf.round_count * conf.tag_count;
  for(int z = 0; z < loops; ++z){
    int y = z % conf.tag_count;
    double it_mean = conf->mean * (double)(y + 1);
    std::normal_distribution<double> distribution(it_mean, conf->std_dev);
    for(int i = 0; i < conf->op_count; ++i){
      int index = (int)distribution(generator);
      librados::bufferlist bl;
      const char * sample = dgen.sample();
      bl.append(sample, size);
      bl.append("v2");
      librados::ObjectWriteOperation write_op;
      write_op.write_full(bl);
      if( std::abs(index - it_mean) < (int)(conf->std_dev * conf->tag_dev)){
        librados::bufferlist tag_bl;
        tag_bl.append("tag");
        std::stringstream tag_ss;
        tag_ss << "BP_TAG_" << y;
        write_op.setxattr(tag_ss.str().c_str(), tag_bl);
        std::cout << tag_ss.str().c_str() << std::endl;
        std::cout << "tagged index: " << index << "." << y << std::endl;
      } else {
        std::cout << "not tagged index: " << index << "." << y << std::endl;
      }
      std::stringstream ss;
      ss << object_name << "." << index; 
      auto start_us = getus();
      //std::this_thread::sleep_for(std::chrono::milliseconds(2));   
      //int ret = 0; 
      int ret = conf->io_ctx.operate(ss.str(), &write_op);
      auto latency_us = getus() - start_us;
      if (ret < 0) {
        std::cerr << "failed to do compound write! error " << ret << std::endl;
        break;
      } else {
        // std::cout << "we wrote our object " << object_name << " with contents\n" << bl.c_str() << std::endl;
        conf->write_map[index] = true;
        hdr_record_value(conf->write_histogram, latency_us);
        write_ps.fetch_add(1);
      }
    }
    is_done.fetch_add(1);
  }
}

void send_read_reqs(conf_t* conf){
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  std::string object_name("hello_object");

  std::default_random_engine generator;
  generator.seed(std::chrono::system_clock::now().time_since_epoch().count());
  std::normal_distribution<double> distribution(conf->mean, conf->std_dev);


  while(is_done.load() <= 0){
    int index = (int)distribution(generator);
    if(!conf->write_map[index]){
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    librados::bufferlist bl;
    std::stringstream ss;
    ss << object_name << "." << index;
    auto start_us = getus();
    //std::this_thread::sleep_for(std::chrono::milliseconds(2));
    //int ret = 0;
    int ret = conf->io_ctx.read(ss.str(), bl, conf->data_size, 0);
    auto latency_us = getus() - start_us;
    if (ret < 0) {
      std::cerr << "failed to read! error " << ret << std::endl;
      break;
    } else {
      // std::cout << "read our object " << object_name << std::endl;
      hdr_record_value(conf->read_histogram, latency_us);
      read_ps.fetch_add(1);
    }
  }
}

void report(conf_t* conf){
  while(true){
    if(conf->exit_reporter){
      break;
    }

    std::cout << "Read iops: " << read_ps.load() << std::endl;
    std::cout << "Write iops: " << write_ps.load() << std::endl;

    read_ps.store(0);
    write_ps.store(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}


int main(int argc, const char **argv)
{
  std::string pool_name;
  bool read;
  bool write;
  int n_threads;
  int n_ops;
  int n_rounds;

  po::options_description opts("Benchmark options");
  opts.add_options()
  ("poolname,p", po::value<std::string>(&pool_name)->default_value("wbbench"), "Log name")
  ("read,r", po::bool_switch(&read)->default_value(false), "perform reads")
  ("write,w", po::bool_switch(&writes)->default_value(false), "perform writes")
  ("threads,t", po::value<int>(&n_threads)->default_value(1), "number of threads")
  ("ops,p", po::value<int>(&n_ops)->default_value(100), "number of ops")
  ("rounds,p", po::value<int>(&n_rounds)->default_value(3), "number of rounds")
  ("tags,g", po::value<int>(&n_rounds)->default_value(3), "number of tags")
  ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, opts), vm);

  if (vm.count("help")) {
    std::cout << opts << std::endl;
    return 1;
  }

  po::notify(vm);



  int ret = 0;

  // we will use all of these below
  librados::IoCtx io_ctx;

  // first, we create a Rados object and initialize it
   librados::Rados rados;
  {
    ret = rados.init("admin"); // just use the client.admin keyring
    if (ret < 0) { // let's handle any error that might have come back
      std::cerr << "couldn't initialize rados! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      rados.shutdown();
      exit(-1);
    } else {
      std::cout << "we just set up a rados cluster object" << std::endl;
    }
  }

  /*
   * Now we need to get the rados object its config info. It can
   * parse argv for us to find the id, monitors, etc, so let's just
   * use that.
   */
  {
    ret = rados.conf_parse_argv(argc, argv);
    if (ret < 0) {
      // This really can't happen, but we need to check to be a good citizen.
      std::cerr << "failed to parse config options! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      rados.shutdown();
      exit(-1);
    } else {
      std::cout << "we just parsed our config options" << std::endl;
      // We also want to apply the config file if the user specified
      // one, and conf_parse_argv won't do that for us.
      for (int i = 0; i < argc; ++i) {
	if ((strcmp(argv[i], "-c") == 0) || (strcmp(argv[i], "--conf") == 0)) {
	  ret = rados.conf_read_file(argv[i+1]);
	  if (ret < 0) {
	    // This could fail if the config file is malformed, but it'd be hard.
	    std::cerr << "failed to parse config file " << argv[i+1]
	              << "! error" << ret << std::endl;
	    ret = EXIT_FAILURE;
	    rados.shutdown();
      exit(-1);
	  }
	  break;
	}
      }
    }
  }

  /*
   * next, we actually connect to the cluster
   */
  {
    ret = rados.connect();
    if (ret < 0) {
      std::cerr << "couldn't connect to cluster! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      rados.shutdown();
      exit(-1);
    } else {
      std::cout << "we just connected to the rados cluster" << std::endl;
    }
  }


  /*
   * create an "IoCtx" which is used to do IO to a pool
   */
  {
    ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
    if (ret < 0) {
      std::cerr << "couldn't set up ioctx! error " << ret << std::endl;
      ret = EXIT_FAILURE;
      rados.shutdown();
      exit(-1);
    } else {
      std::cout << "we just created an ioctx for our pool" << std::endl;
    }
  }

  bool* write_map = (bool*)malloc(sizeof(bool) * 10000);

  struct hdr_histogram* read_histogram;
  struct hdr_histogram* write_histogram;

  // Initialise the histogram
  hdr_init(
  1,  // Minimum value
  INT64_C(3600000000),  // Maximum value
  3,  // Number of significant figures
  &read_histogram);  // Pointer to initialise

  hdr_init(
  1,  // Minimum value
  INT64_C(3600000000),  // Maximum value
  3,  // Number of significant figures
  &write_histogram);  // Pointer to initialise

  conf_t conf = {
    io_ctx,             //io_ctx
    write_map,          //write_map
    "this is the data", //object_data
    16,                 //data_size
    500.0,              //mean
    40.0,               //std_dev
    1.0,                //tag_dev
    100,               //op_count
    read_histogram,     //read histogram
    write_histogram,    //write histogram
    false               //exit reporter
   };

  std::vector<std::thread> threads;

  if(writes){
    for(size_t i = 0; i < n_threads; i++)
    {
      std::thread write_thread(send_write_reqs, &conf);
      threads.push_back(write_thread);
    }
  }

  if(reads){
    for(size_t i = 0; i < n_threads; i++)
    {
      std::thread read_thread(send_read_reqs, &conf);
      threads.push_back(read_thread);
    }
  }

  for(auto thread : threads){
      thread.join();
  }

  //std::thread reporter(report, &conf);
  //conf.exit_reporter = true;
  //reporter.join();

  rados.shutdown();

  std::stringstream fnr;
  fnr << n_ops << "r-ops-" << n_rounds << "-rds-" << n_threads << "-thrds" << ".latency.csv";
  FILE *ltfr = fopen(fn.str().c_str(), "w");
  std::cout << "READS: " << std::endl;
  hdr_percentiles_print(
    read_histogram,
    ltfr,  // File to write to
    5,  // Granularity of printed values
    1.0,  // Multiplier for results
    CLASSIC);
  
  std::stringstream fnw;
  fnw << n_ops << "w-ops-" << n_rounds << "-rds-" << n_threads << "-thrds" << ".latency.csv";
  FILE *ltfw = fopen(fn.str().c_str(), "w");
  std::cout << "WRITE: " << std::endl;
  hdr_percentiles_print(
    write_histogram,
    ltfw,  // File to write to
    5,  // Granularity of printed values
    1.0,  // Multiplier for results
    CLASSIC);

  return 0;
}
