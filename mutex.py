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
option = 0

class Node():
	"""
	Node Class: contains functionality for a node in the Maekawa-esque network
	"""

	def __init__(self, nodeID, cs_int, next_req):
		self.nodeID = nodeID
		self.next_req = int(next_req)
		self.cs_int = int(cs_int)
		self.vote_size = int(math.sqrt(N))
		self.voting_set = set()
		for i in range(1, self.vote_size + 1):
			self.voting_set.add((nodeID + i)%N)
		self.queued_reqs = Queue.PriorityQueue()
		self.has_replied = False
		self.grant_req = None
		self.reset_params()

		# create thread to listen for incoming messages
		listenThread = threading.Thread(target=self.__listen)
		listenThread.setDaemon(True)
		listenThread.start()

	def __str__(self):
		result = "Node: " + str(self.nodeID) + "\n"
		return result

	def init(self):
		# wait for other nodes to get set up
		while len(message_queues) < N:
			time.sleep(0.01)

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

		# wait to receive all votes, TODO: handle deadlock
		while len(self.grants_received) < self.vote_size:
			time.sleep(0.01)		

		self.in_cs = True

		# print for logging
		result = "{0:.6f} {1}".format(time.time(), self.nodeID)
		for voter in self.voting_set:
			result += " " + str(voter)
		print result

		if DEBUG_MODE:
			print str(self.nodeID) + " has entered CS "

	def __exit_cs(self):
		# send out release messages to members of voting set
		release_msg = Message("release", time.time(), self.nodeID)
		for nodeID in self.voting_set:
			message_queues[nodeID].put((release_msg.ts, release_msg))

		self.reset_params()
		if DEBUG_MODE:
			print str(self.nodeID) + " has exited CS "

	def reset_params(self):
		self.grants_received = {}
		self.in_cs = False
		self.recv_failed = False
		self.yield_no_grant = set()

	def process_message(self, message):
		if message.msg == "request":
			# if we've already replied before the next release, queue requests
			if self.has_replied:
				self.queued_reqs.put((message.ts, message))
				if DEBUG_MODE:
					print str(self.nodeID) + ": " + str(self.grant_req) + ", " + str(message.src_nodeID)
				if self.grant_req.ts > message.ts:
					msg = Message("failed", time.time(), self.nodeID)
					message_queues[message.src_nodeID].put((msg.ts, msg))
				else:
					msg = Message("inquire", time.time(), self.nodeID)
					message_queues[self.grant_req.src_nodeID].put((msg.ts, msg))

			# else, grant this request
			else:
				grant_msg = Message("grant", time.time(), self.nodeID)
				message_queues[message.src_nodeID].put((grant_msg.ts, grant_msg))

				# indicate we have replied before receiving a release msg
				self.has_replied = True
				self.grant_req = message

		elif message.msg == "grant":
			# record grant
			self.grants_received[message.src_nodeID] = message
			if message.src_nodeID in self.yield_no_grant:
				self.yield_no_grant.remove(message.src_nodeID)

		elif message.msg == "release":
			# if we have no queued requests, reset has_replied flag
			if self.queued_reqs.empty():
				self.has_replied = False
				self.grant_req = None

			# else, handle next queued req
			else:
				next_req = self.queued_reqs.get()[1]
				grant_msg = Message("grant", time.time(), self.nodeID)
				message_queues[next_req.src_nodeID].put((grant_msg.ts, grant_msg))

				# indicate we have replied
				self.has_replied = True
				self.grant_req = next_req

		elif message.msg == "inquire":
			if not self.in_cs and (self.recv_failed or len(self.yield_no_grant) > 0):
				grant_msg = Message("yield", time.time(), self.nodeID)
				message_queues[message.src_nodeID].put((grant_msg.ts, grant_msg))
				self.yield_no_grant.add(message.src_nodeID)
				del self.grants_received[message.src_nodeID]
			# del self.grants_received[message.src_nodeID]
			# time.sleep(0.05)
			# if self.in_cs:
			# 	self.grants_received[message.src_nodeID] = message
			# else:
			# 	if self.recv_failed or self.yield_no_grant:
			# 		grant_msg = Message("yield", time.time(), self.nodeID)
			# 		message_queues[message.src_nodeID].put(grant_msg.ts, grant_msg)
			# 		self.yield_no_grant = True
			# 	else:
			# 		self.grants_received[message.src_nodeID] = message
			
		elif message.msg == "yield":
			next_req = self.queued_reqs.get()[1]
			grant_msg = Message("grant", time.time(), self.nodeID)
			message_queues[next_req.src_nodeID].put((grant_msg.ts, grant_msg))

			message.msg = "request"
			self.queued_reqs.put((message.ts, message))

			# indicate we have replied
			self.has_replied = True
			self.grant_req = next_req

		elif message.msg == "failed":
			self.recv_failed = True

	def __listen(self):
		global option
		while True:
			time.sleep(0.01)

			# if we've received data, process it
			if not message_queues[self.nodeID].empty():
				msg = message_queues[self.nodeID].get()[1]
				
				# print for logging
				if option:
					result = "{0:.6f} {1} {2} {3}".format(time.time(), self.nodeID, msg.src_nodeID, msg.msg)
					print result

				# process the message
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
		result += "ts: " + "{0:.6f}".format(self.ts) + ", "
		result += "src_nodeID: " + str(self.src_nodeID)
		return result

# Usage: python mutex.py <cs_int> <next_req> <tot_exec_time> <option>
if __name__ == "__main__":
	if(len(sys.argv) < 5):
		print "Usage: python mutex.py <cs_int> <next_req> <tot_exec_time> <option>"
		sys.exit()

	global option

	cs_int = sys.argv[1]
	next_req = sys.argv[2]
	tot_exec_time = sys.argv[3]
	option = int(sys.argv[4])

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

	if DEBUG_MODE:
		print "=== Total Execution Time Reached ==="

