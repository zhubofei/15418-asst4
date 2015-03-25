#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include <sstream>
#include <queue>
#include <iterator>

#include "server/messages.h"
#include "server/master.h"

#define MAX_THREADS 23

typedef struct {
  int first_tag;
  int finished_count;
  int n[4];
} Crequest;

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.
  bool server_ready;
  int max_num_workers;
  int num_pending_client_requests;
  int next_tag;
  int pending_worker_num;

  std::queue<Request_msg> holding_requests;
  // count of pending requests
  std::unordered_map<Worker_handle, int> my_workers;
  std::unordered_map<int, std::string> request_msg_strings;
  std::unordered_map<int, Client_handle> waiting_clients;
  std::unordered_map<std::string, Response_msg> cached_responses;
  std::unordered_map<int, Crequest*> compareprimes_requests;
} mstate;

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

void assign_request(const Request_msg&);

// -----------------------------------------------------------------------

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.pending_worker_num = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker

  int tag = random();
  Request_msg req(tag);
  req.set_arg("name", "my worker 0");
  request_new_worker_node(req);

  // increase number of pending worker
  mstate.pending_worker_num++;
}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.
  // decrease pending worker num
  mstate.pending_worker_num--;

  // register new worker
  mstate.my_workers[worker_handle] = 0;

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  mstate.my_workers[worker_handle]--;
  if (mstate.compareprimes_requests.count(resp.get_tag())) {
    Crequest* crequest = mstate.compareprimes_requests[resp.get_tag()];
    crequest->finished_count++;
    crequest->n[resp.get_tag() - crequest->first_tag] = atoi(resp.get_response().c_str());
    mstate.compareprimes_requests.erase(resp.get_tag());
    mstate.cached_responses[mstate.request_msg_strings[resp.get_tag()]] = resp;

    // if dummy requests all finished, send respond
    if (crequest->finished_count == 4) {
      Response_msg new_resp(crequest->first_tag);
      if (crequest->n[1] - crequest->n[0] > crequest->n[3] - crequest->n[2])
        new_resp.set_response("There are more primes in first range.");
      else
        new_resp.set_response("There are more primes in second range.");

      send_client_response(mstate.waiting_clients[crequest->first_tag], new_resp);
      mstate.waiting_clients.erase(crequest->first_tag);
      delete crequest;
    }
    mstate.request_msg_strings.erase(resp.get_tag());
  } else {
    send_client_response(mstate.waiting_clients[resp.get_tag()], resp);
    // cache
    mstate.cached_responses[mstate.request_msg_strings[resp.get_tag()]] = resp;
    // delete
    mstate.request_msg_strings.erase(resp.get_tag());
    mstate.waiting_clients.erase(resp.get_tag());
  }
  // always decrease pending request by one
  mstate.num_pending_client_requests--;
}

void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }

  // if request is compareprimes, split into four requests
  if (client_req.get_arg("cmd").compare("compareprimes") == 0) {
    int params[4];

    // grab the four arguments defining the two ranges
    params[0] = atoi(client_req.get_arg("n1").c_str());
    params[1] = atoi(client_req.get_arg("n2").c_str());
    params[2] = atoi(client_req.get_arg("n3").c_str());
    params[3] = atoi(client_req.get_arg("n4").c_str());

    // four dummy request shared the same crequest struct
    Crequest* crequest = new Crequest;
    crequest->first_tag = mstate.next_tag;
    crequest->finished_count = 0;
    mstate.waiting_clients[mstate.next_tag] = client_handle;

    // create new requests
    for (int i=0; i<4; i++) {
      // update tag
      int tag = mstate.next_tag++;
      // create dummy request
      Request_msg dummy_req(tag);
      create_computeprimes_req(dummy_req, params[i]);

      // check cache
      if (mstate.cached_responses.count(dummy_req.get_request_string())) {
        Response_msg resp(tag);
        // update crequest
        resp.set_response(mstate.cached_responses[dummy_req.get_request_string()].get_response());
        crequest->finished_count++;
        crequest->n[i] = atoi(resp.get_response().c_str());
      } else {
        // save requst string to the map
        mstate.request_msg_strings[tag] = dummy_req.get_request_string();
        // save request to the map
        mstate.compareprimes_requests[tag] = crequest;
        // send request
        assign_request(dummy_req);
        // increase num of pennding request
        mstate.num_pending_client_requests++;
      }
    }

    // if all parts are completed, create response
    if (crequest->finished_count == 4) {
      Response_msg resp(crequest->first_tag);

      if (crequest->n[1] - crequest->n[0] > crequest->n[3] - crequest->n[2])
        resp.set_response("There are more primes in first range.");
      else
        resp.set_response("There are more primes in second range.");

      send_client_response(client_handle, resp);
      delete crequest;
    } else { // wait
      // save client_handle and wait
      mstate.waiting_clients[crequest->first_tag] = client_handle;
    }

  } else if (mstate.cached_responses.count(client_req.get_request_string())) {
    Response_msg resp(mstate.next_tag++);
    resp.set_response(mstate.cached_responses[client_req.get_request_string()].get_response());
    send_client_response(client_handle, resp);
  } else {
    // New tag
    int tag = mstate.next_tag++;
    // Save off the handle to the client that is expecting a response.
    // The master needs to do this it can response to this client later
    // when 'handle_worker_response' is called.
    mstate.waiting_clients[tag] = client_handle;
    mstate.request_msg_strings[tag] = client_req.get_request_string();
    mstate.num_pending_client_requests++;

    // Fire off the request to the worker.  Eventually the worker will
    // respond, and your 'handle_worker_response' event handler will be
    // called to forward the worker's response back to the server.
    Request_msg worker_req(tag, client_req);
    assign_request(worker_req);

  }

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.
}

void assign_request(const Request_msg& req) {
  auto w = mstate.my_workers.begin();
  send_request_to_worker(w->first, req);
  w->second++;
  /*for (auto& w: mstate.my_workers) {
    send_request_to_worker(w.first, req);
    w.second++;
    break;
  }*/
}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

}
