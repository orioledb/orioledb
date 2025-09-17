import inspect
from itertools import repeat
import sys
from types import MethodType
import os

from multiprocessing.pool import ThreadPool
from unittest import TextTestResult
from unittest.runner import _WritelnDecorator

from ..t.base_test import BaseTest
from .utils import normalize_name, pg_regress


class ParralelTextTestResult(TextTestResult):

	def __init__(self):
		super().__init__(_WritelnDecorator(sys.stderr), True, 2)

	def startTest(self, test):
		super(TextTestResult, self).startTest(test)
		self.stream.write("\n" + self.getDescription(test) + " started \n")
		self.stream.flush()
		self._newline = False

	def printParallelTestStatus(self, test, status):
		res = "\n" + self.getDescription(test)
		res += f" finished: {status}"
		if len(self.collectedDurations) > 0:
			res += ": %.3fs" % self.collectedDurations[0][1]
		res += "\n"
		self.stream.write(res)
		self.stream.flush()

	def _write_status(self, test, status):
		self.printParallelTestStatus(test, status)
		self._newline = False

	def addExpectedFailure(self, test, err):
		super(TextTestResult, self).addExpectedFailure(test, err)
		self.printParallelTestStatus(test, "expected failure")
		self._newline = False

	def addUnexpectedSuccess(self, test):
		super(TextTestResult, self).addUnexpectedSuccess(test)
		self.printParallelTestStatus(test, "unexpected success")
		self._newline = False


class GroupBase(BaseTest):

	@property
	def myName(self):
		if not self._myName:
			number = self.__class__.__name__.removeprefix("Group")
			name = os.path.basename(inspect.getfile(self.__class__))
			if name.endswith('_test.py'):
				name = name[:-8]
			elif name.endswith('.py'):
				name = name[:-3]
			name = name + '-' + number + '-' + self.id().split(
			    '.')[-1].removeprefix('test_')
			self._myName = name
		return self._myName

	def setUp(self):
		super().setUp()
		node = self.node
		node.append_conf(default_table_access_method='orioledb')
		node.start()
		GroupBase.runParallelTest(node, "test_setup")
		GroupBase.runParallelTest(node, "create_index")

	def runTest(self):
		from ._g_r_single_test import Run as SingleTest
		test_stack = SingleTest.getTestStack(self.tests, SingleTest.depends)
		pool = ThreadPool(20)

		for stack_level in test_stack:
			results = pool.starmap(GroupBase.runParallelTest,
			                       zip(repeat(self.node), stack_level))

			succeed = len(stack_level)
			for result in results:
				result.showAll = False
				result.printErrors()
				ok = len(result.errors) == 0 and len(
				    result.failures) == 0 and len(
				        result.unexpectedSuccesses) == 0
				if not ok:
					succeed -= 1
					type(self).has_errors = True
			self.assertParallelTestSucceeded(len(stack_level), succeed)

		pool.close()
		pool.join()

	def assertParallelTestSucceeded(self, expected, real):
		msg = f"Succeeded: {real}/{expected}"
		if expected != real:
			raise self.failureException(msg)
		else:
			print(msg)

	def getTestList(self, test):
		from ._g_r_single_test import Run as SingleTest
		return [(test, test == "test_setup", test
		         in SingleTest.expectedFailures)]

	def runParallelTest(node, name):
		from ._g_r_single_test import Run as SingleTest
		test = SingleTest(normalize_name(name))
		test.node = node
		test.setUp = lambda: None
		test.tearDown = lambda: None
		test.startNode = lambda: None
		test.stopNode = lambda: None
		test.getTestList = MethodType(GroupBase.getTestList, test)
		subresult = ParralelTextTestResult()
		test(subresult)
		return subresult


class SingleBase(BaseTest):
	depends = {}

	def getTestStack(tests, depends):
		test_stack = [tests]
		level = 0
		curelem = 0
		while level < len(test_stack):
			test = test_stack[level][curelem]
			if test in depends:
				if len(test_stack) == level + 1:
					test_stack += [[]]
				if test not in test_stack[level + 1]:
					test_stack[level + 1] += depends[test]
			curelem += 1
			if curelem == len(test_stack[level]):
				curelem = 0
				level += 1
				if level < len(test_stack):
					test_stack[level] = sorted(set(test_stack[level]))
		test_stack.reverse()
		return test_stack

	def startNode(self):
		self.node.append_conf(default_table_access_method='orioledb')
		self.node.start()

	def stopNode(self):
		self.node.stop()

	def getTestList(self, test):
		result = ["test_setup", "create_index"]
		result += [
		    t for stack_level in SingleBase.getTestStack([test], self.depends)
		    for t in stack_level
		]
		result = [(t, t == "test_setup", t in self.expectedFailures
		           and t != test) for t in result]
		return result

	def runTest(self, test):
		node = self.node
		self.startNode()
		test_list = self.getTestList(test)
		for test, first, fail_ok in test_list:
			pg_regress(node, test, first, fail_ok=fail_ok)

		self.stopNode()
