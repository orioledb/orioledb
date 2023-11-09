#!/usr/bin/env python3

import argparse
import boto3
import os
import re
import struct
import testgres

from concurrent.futures import ThreadPoolExecutor
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from threading import Event
from typing import Callable

class OrioledbS3ObjectLoader:
	def parse_args(self):
		epilog = """
			This util uses boto3 under the hood.
			You can set credentials using AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION variables
			Or by using ~/.aws/config file.
			Read for more details:
			https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables
		"""
		parser = argparse.ArgumentParser(usage=argparse.SUPPRESS,
										 epilog=epilog)
		parser.add_argument('--endpoint', dest='endpoint',
							required=True, help="AWS url")
		parser.add_argument('-d', '--data-dir', dest='data_dir',
							required=True, help="Destination data directory")
		parser.add_argument('--bucket-name', dest='bucket_name',
							required=True, help="Bucket name")
		parser.add_argument('--cert-file', dest='cert_file',
							help="Path to crt file")
		parser.add_argument('--verbose', dest='verbose', action='store_true',
							help="More verbose output. Downloaded files displayed.")

		try:
			args = parser.parse_args()
		except SystemExit as e:
			if e.code != 0:
				parser.print_help()
			raise

		if 'cert_file' in args:
			verify = args.cert_file
		else:
			verify = None

		self.s3 = boto3.client("s3", endpoint_url=args.endpoint,
							   verify=verify)
		self._error_occurred = Event()
		self.data_dir = args.data_dir
		self.bucket_name = args.bucket_name
		self.verbose = args.verbose

	def run(self):
		wal_dir = os.path.join(self.data_dir, 'pg_wal')
		loader.download_files_in_directory(self.bucket_name, 'data/',
										   self.data_dir)
		loader.download_files_in_directory(self.bucket_name,
										   'orioledb_data/',
										   f"{self.data_dir}/orioledb_data",
										   suffix='',
										   transform=self.transform_orioledb)

		control = get_control_data(self.data_dir)
		wal_file = control["Latest checkpoint's REDO WAL file"]
		loader.download_file(self.bucket_name, f"wal/{wal_file}",
							 f"{wal_dir}/{wal_file}")

	def list_objects_last_checkpoint(self, bucket_name, directory):
		objects = []
		paginator = self.s3.get_paginator('list_objects_v2')

		greatest_number = -1
		greatest_number_dir = None
		for page in paginator.paginate(Bucket=bucket_name, Prefix=directory,
									   Delimiter='/'):
			if 'CommonPrefixes' in page:
				for prefix in page['CommonPrefixes']:
					prefix_key = prefix['Prefix'].rstrip('/')
					subdirectory = prefix_key.split('/')[-1]
					try:
						number = int(subdirectory)
						if number > greatest_number:
							greatest_number = number
							greatest_number_dir = prefix['Prefix']
					except ValueError:
						pass
		if greatest_number_dir:
			objects = self.list_objects(bucket_name, greatest_number_dir)

		return objects

	def list_objects(self, bucket_name, directory):
		objects = []
		paginator = self.s3.get_paginator('list_objects_v2')

		for page in paginator.paginate(Bucket=bucket_name, Prefix=directory):
			if 'Contents' in page:
				page_objs = [x["Key"] for x in page['Contents']]
				objects.extend(page_objs)

		return objects

	# Reimplement os.dirs so it sets mode for intermediate dirs also
	def makedirs(self, name, mode=0o777, exist_ok=False):
		"""makedirs(name [, mode=0o777][, exist_ok=False])

		Super-mkdir; create a leaf directory and all intermediate ones.  Works like
		mkdir, except that any intermediate path segment (not just the rightmost)
		will be created if it does not exist. If the target directory already
		exists, raise an OSError if exist_ok is False. Otherwise no exception is
		raised.  This is recursive.

		"""
		head, tail = os.path.split(name)
		if not tail:
			head, tail = os.path.split(head)
		if head and tail and not os.path.exists(head):
			try:
				self.makedirs(head, mode, exist_ok=exist_ok)
			except FileExistsError:
				# Defeats race condition when another thread created the path
				pass
			cdir = os.curdir
			if isinstance(tail, bytes):
				cdir = bytes(os.curdir, 'ASCII')
			if tail == cdir:           # xxx/newdir/. exists if xxx/newdir exists
				return
		try:
			os.mkdir(name, mode)
		except OSError:
			# Cannot rely on checking for EEXIST, since the operating system
			# could give priority to other errors like EACCES or EROFS
			if not exist_ok or not os.path.isdir(name):
				raise

	def download_file(self, bucket_name, file_key, local_path):
		try:
			transfer_config = TransferConfig(use_threads=False,
											 max_concurrency=1)
			if file_key[-1] == '/':
				dirs = local_path
			else:
				dirs = '/'.join(local_path.split('/')[:-1])
			self.makedirs(dirs, exist_ok=True, mode=0o700)
			if file_key[-1] != '/':
				self.s3.download_file(
					bucket_name, file_key, local_path, Config=transfer_config
				)
			if self.verbose:
				print(f"{file_key} -> {local_path}", flush=True)
			if re.match(r'.*/orioledb_data/small_files_\d+$', local_path):
				base_dir = '/'.join(local_path.split('/')[:-2])
				with open(local_path, 'rb') as file:
					data = file.read()
				numFiles = struct.unpack('i', data[0:4])[0]
				for i in range(0, numFiles):
					(nameOffset, dataOffset, dataLength) = struct.unpack('iii', data[4 + i * 12: 16 + i * 12])
					name = data[nameOffset: data.find(b'\0', nameOffset)].decode('ascii')
					fullname = f"{base_dir}/{name}"
					if self.verbose:
						print(f"{file_key} -> {fullname}", flush=True)
					self.makedirs(os.path.dirname(fullname), exist_ok=True, mode=0o700)
					with open(fullname, 'wb') as file:
						file.write(data[dataOffset: dataOffset + dataLength])
					os.chmod(fullname, 0o600)
				os.unlink(local_path)

		except ClientError as e:
			if e.response['Error']['Code'] == "404":
				print(f"File not found: {file_key}")
			else:
				print(f"An error occurred: {e}")
			self._error_occurred.set()

	def transform_orioledb(self, val: str) -> str:
		parts = val.split('/')
		file_parts = parts[3].split('.')
		result = f"{parts[2]}/{file_parts[0]}-{parts[1]}"
		if file_parts[-1] == 'map':
			result += '.map'
		return result

	def transform_pg(val: str) -> str:
		return '/'.join(val.split('/')[2:])

	def download_files_in_directory(self, bucket_name, directory,
									local_directory, suffix='',
									transform: Callable[[str], str] = transform_pg):
		objects = self.list_objects_last_checkpoint(bucket_name, directory)
		max_threads = os.cpu_count()

		with ThreadPoolExecutor(max_threads) as executor:
			futures = []

			for file_key in objects:
				if not file_key.endswith(suffix):
					continue
				local_file = transform(file_key)
				local_path = f"{local_directory}/{local_file}"
				future = executor.submit(self.download_file, bucket_name,
										 file_key, local_path)
				futures.append(future)

			for future in futures:
				future.result()

				if self._error_occurred.is_set():
					print("An error occurred. Stopping all downloads.")
					executor.shutdown(wait=False, cancel_futures=True)
					break

def get_control_data(data_dir: str):
	"""
	Return contents of pg_control file.
	"""

	# this one is tricky (blame PG 9.4)
	_params = [testgres.get_bin_path("pg_controldata")]
	_params += ["-D"]
	_params += [data_dir]

	data = testgres.utils.execute_utility(_params)

	out_dict = {}

	for line in data.splitlines():
		key, _, value = line.partition(':')
		out_dict[key.strip()] = value.strip()

	return out_dict

if __name__ == '__main__':
	loader = OrioledbS3ObjectLoader()
	loader.parse_args()
	loader.run()
