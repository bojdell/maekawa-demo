#!/usr/bin/python

import socket
import sys
import threading
import time
import Queue
import math

message_queues = {}
N = 9
DEBUG_MODE = False

class Node():
	"""
	Node Class: contains functionality for a node in the Maekawa-esque network
	"""

	def __init__(self, nodeID, next_req, cs_int):
		self.nodeID = nodeID
		self.next_req = int(next_req)
		self.cs_int = int(cs_int)
		self.vote_size = int(math.sqrt(N))
		self.voting_set = set()
		for i in range(1, self.vote_size + 1):
			self.voting_set.add((nodeID + i)%N)
		self.replies_received = {}
		self.queued_reqs = Queue.PriorityQueue()
		self.has_replied = False
		self.in_cs = False

	def __str__(self):
		result = "Node: " + str(self.nodeID) + "\n"
		return result

	def init(self):
		# wait for other nodes to get set up
		while len(message_queues) < N:
			pass

		# create thread to listen for incoming messages
		listenThread = threading.Thread(target=self.__listen)
		listenThread.setDaemon(True)
		listenThread.start()

		# create thread to process state machine
		smThread = threading.Thread(target=self.__init)
		smThread.setDaemon(True)
		smThread.start()

	def __init(self):
		# loop forever in state machine (starting in request state)
		while True:
			self.request()

	def request(self):
		# enter cs
		self.__enter_cs()

		# move to held state
		self.held()

	def held(self):
		# wait for cs_int milliseconds
		start = int(round(time.time() * 1000))
		while(int(round(time.time() * 1000)) - start < self.cs_int):
			time.sleep(0.01)

		# move to release state
		self.release()

	def release(self):
		# exit cs
		self.__exit_cs()

		# wait for next_req milliseconds
		start = int(round(time.time() * 1000))
		while(int(round(time.time() * 1000)) - start < self.next_req):
			time.sleep(0.01)

		# return to request state
		return

	def __enter_cs(self):
		# send out request messages to members of voting set
		req_msg = Message("request", time.time(), self.nodeID)
		for nodeID in self.voting_set:
			message_queues[nodeID].put((req_msg.ts, req_msg))
			if DEBUG_MODE:
				print str(self.nodeID) + " request -> " + str(nodeID)

		# wait to receive all votes, TODO: handle deadlock
		while len(self.replies_received) < self.vote_size:
			time.sleep(0.01)

		self.in_cs = True

	def __exit_cs(self):
		# send out release messages to members of voting set
		release_msg = Message("release", time.time(), self.nodeID)
		for nodeID in self.voting_set:
			message_queues[nodeID].put((release_msg.ts, release_msg))
			if DEBUG_MODE:
				print str(self.nodeID) + " release -> " + str(nodeID)

		self.reset_params()

	def reset_params(self):
		self.replies_received = {}
		self.in_cs = False
		self.recv_failed = False
		self.yield_no_grant = False
		self.grant_priority = None

	def process_message(self, message):
		if message.msg is "request":
			# if we've already replied before the next release, queue requests
			if self.has_replied:
				self.queued_reqs.put((message.src_nodeID, message))
				if self.grant_priority and self.grant_priority > message.src_nodeID:
					msg = Message("failed", time.time(), self.nodeID)
					message_queues[message.src_nodeID].put((msg.ts, msg))
					if DEBUG_MODE:
						print str(self.nodeID) + " failed -> " + str(message.src_nodeID)
				else:
					msg = Message("inquire", time.time(), self.nodeID)
					message_queues[self.grant_priority].put((msg.ts, msg))

			# else, grant this request
			else:
				grant_msg = Message("grant", time.time(), self.nodeID)
				message_queues[message.src_nodeID].put((grant_msg.ts, grant_msg))
				self.grant_priority = message.src_nodeID
				if DEBUG_MODE:
					print str(self.nodeID) + " grant -> " + str(message.src_nodeID)
				
				# indicate we have replied before receiving a release msg
				self.has_replied = True

		elif message.msg is "grant":
			# record grant
			replies_received[message.src_nodeID] = message
			self.yield_no_grant = False

		elif message.msg is "release":
			# if we have no queued requests, reset has_replied flag
			if self.queued_reqs.empty():
				self.has_replied = False

			# else, handle next queued req
			else:
				next_req = self.queued_reqs.get()[1]
				grant_msg = Message("grant", time.time(), self.nodeID)
				message_queues[next_req.src_nodeID].put((grant_msg.ts, grant_msg))
				if DEBUG_MODE:
					print str(self.nodeID) + " grant -> " + str(next_req.src_nodeID)

				# indicate we have replied
				self.has_replied = True

		elif message.msg is "inquire":
			del replies_received[message.src_nodeID]
			time.sleep(0.05)
			if self.in_cs:
				replies_received[message.src_nodeID] = message
			else:
				if self.recv_failed or self.yield_no_grant:
					grant_msg = Message("yield", time.time(), self.nodeID)
					message_queues[message.src_nodeID].put(grant_msg.ts, grant_msg)
					self.yield_no_grant = True
				else:
					replies_received[message.src_nodeID] = message
			
		elif message.msg is "yield":
			next_req = self.queued_reqs.get()[1]
			grant_msg = Message("grant", time.time(), self.nodeID)
			self.grant_priority = next_req.src_nodeID
			message_queues[next_req.src_nodeID].put((grant_msg.ts, grant_msg))

			message.msg = "request"
			self.queued_reqs.put((message.src_nodeID, message))

		elif message.msg is "failed":
			self.recv_failed = True

	def __listen(self):
		while True:
			time.sleep(0.01)

			# if we've received data, process it
			if not message_queues[self.nodeID].empty():
				msg = message_queues[self.nodeID].get()[1]
				if DEBUG_MODE:
					print "msg received at node " + str(self.nodeID) + ": " + str(msg)
				self.process_message(msg)

class Message():
	"""
	Class that is used to communicate between nodes in the network
	"""

	def __init__(self, msg, ts, src_nodeID):
		self.msg = str(msg).lower() if msg else None
		self.ts = ts if ts else None
		self.src_nodeID = src_nodeID

	def __str__(self):
		result = "msg: " + str(self.msg) + ", "
		result += "ts: " + str(self.ts) + ", "
		result += "src_nodeID: " + str(self.src_nodeID)
		return result

# Usage: python mutex.py <cs_int> <next_req> <tot_exec_time> <option>
if __name__ == "__main__":
	if(len(sys.argv) < 5):
		print "Usage: python mutex.py <cs_int> <next_req> <tot_exec_time> <option>"
		sys.exit()

	cs_int = sys.argv[1]
	next_req = sys.argv[2]
	tot_exec_time = sys.argv[3]
	option = sys.argv[4]

	nodes = []
	
	for i in range(0, N):
		# init message passing channel
		message_queues[i] = Queue.PriorityQueue()

		# init node
		nodes.append(Node(i, cs_int, next_req))

	if DEBUG_MODE:
		raw_input("press enter...")

	for i in range(0, N):
		nodes[i].init()

	start = time.time()
	while time.time() - start < float(tot_exec_time):
		time.sleep(0.01)

	print "=== Total Execution Time Reached ==="

