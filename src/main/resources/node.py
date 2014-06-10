import pickle

class Node:
	def __init__(self, metric_path, isLeaf):
		self.metric_path = metric_path
		self.isLeaf = isLeaf

