#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <sstream>
#include <glog/logging.h>

//#include <iostream>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "tools/work_queue.h"

#define MAX_THREADS_NUM 23 // leave one for tellmenow

WorkQueue<Request_msg> work_queue;
WorkQueue<Request_msg> tell_queue;

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// Implements logic required by compareprimes command via multiple
// calls to execute_work.  This function fills in the appropriate
// response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int params[4];
    int counts[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(req.get_arg("n1").c_str());
    params[1] = atoi(req.get_arg("n2").c_str());
    params[2] = atoi(req.get_arg("n3").c_str());
    params[3] = atoi(req.get_arg("n4").c_str());

    for (int i=0; i<4; i++) {
      Request_msg dummy_req(0);
      Response_msg dummy_resp(0);
      create_computeprimes_req(dummy_req, params[i]);
      execute_work(dummy_req, dummy_resp);
      counts[i] = atoi(dummy_resp.get_response().c_str());
    }

    if (counts[1]-counts[0] > counts[3]-counts[2])
      resp.set_response("There are more primes in first range.");
    else
      resp.set_response("There are more primes in second range.");
}

// For the use of pthread
void* worker_thread(void* arg) {
   while (1) {
     Request_msg req = work_queue.get_work();
     Response_msg resp(req.get_tag());

     if (req.get_arg("cmd").compare("compareprimes") == 0) {
       // The compareprimes command needs to be special cased since it is
       // built on four calls to execute_execute work.  All other
       // requests from the client are one-to-one with calls to
       // execute_work.
       execute_compareprimes(req, resp);
     } else {
       // actually perform the work.  The response string is filled in by
       // 'execute_work'
       execute_work(req, resp);
     }
     worker_send_response(resp);
   }
   return NULL;
}

void* tell_thread(void* arg) {
   while (1) {
     Request_msg req = tell_queue.get_work();
     Response_msg resp(req.get_tag());
     execute_work(req, resp);
     worker_send_response(resp);
   }
   return NULL;
}

void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

  // One thread here as the master thread
  for (int i=0; i<MAX_THREADS_NUM; i++) {
    pthread_t t;
    pthread_create(&t, NULL, &worker_thread, NULL);
  }
  // leave one high way for tellmenow
  pthread_t t;
  pthread_create(&t, NULL, &tell_thread, NULL);
}

void worker_handle_request(const Request_msg& req) {

  // Output debugging help to the logs (in a single worker node
  // configuration, this would be in the log logs/worker.INFO)
  DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

  if (req.get_arg("cmd").compare("tellmenow") == 0) {
    tell_queue.put_work(req);
  } else {
    work_queue.put_work(req);
  }
}
