#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os, sys, hashlib

import queue
import psutil

import multiprocessing, threading

from threading import Thread
from multiprocessing import Process, Value, Queue
# multiprocessing is fucked up on windows

from pydelete_utils import timed_tigger, split_list
from multiprogressbar import *


class QueuedFileHasher_mp(Process):
    """Class for hashing a list of files in a separate process."""

    def __init__(self, work_queue, work_queue_pos, algorithm = 'sha1', **kwargs):
        """
        Hash a list of files asyncroniously in a separate process

        Parameters:
            work_queue:         A list of dictionaries like this [{path}, ...]
            work_queue_pos:     An integer of type Value()
            algorithm:          Any algorithm string supported by hashlib

        """
        assert(algorithm in hashlib.algorithms_available), f'hashlib doesnt support the "{algorithm}" algorithm'

        super().__init__(target=self.worker, args=[], **kwargs)

        self.algorithm       = algorithm
        self.work_queue      = work_queue
        self.work_queue_pos  = work_queue_pos
        self.out_queue       = Queue()
        self.read_bytes      = Value('i', 0)
        self.flag_run        = Value('i', 1)

        self.start()


    def stop(self):
        """Set the flag to inform the process to stop. The worker should be joined after to free the resources."""
        self.flag_run.value = 0

    def worker(self):
        self.__hash_func = hashlib.new(self.algorithm)

        def read_to_hash(hash_func, file_obj, read_bytes, chunk):
            data = file_obj.read(chunk)
            hash_func.update(data)
            read_bytes.value += len(data)
            return len(data)
        
        output_buffer = []

        while self.flag_run.value:
            # Exit in case the parent pid is dead
            if not psutil.pid_exists(os.getppid()):
                sys.exit(255)

            error = {'error': []}
            hex_digest = None
            item = None

            # Try to get item
            try:
                if self.work_queue_pos.value < len(self.work_queue):
                    item = self.work_queue[ self.work_queue_pos.value ]
                    item.update({ 'proc_index': self.work_queue_pos.value })
                    self.work_queue_pos.value += 1 
                else:
                    self.flag_run.value = 0
                    continue
            except IndexError as e:
                print(f'{e}\n len{len(self.work_queue)} - pos{self.work_queue_pos.value}')
                raise

            # Open file for reading
            fd = None
            try:
                fd = item['path'].open('rb')
            except (FileNotFoundError, PermissionError, OSError) as e:
                msg = f'Error opening file: {str(item["path"])} \n{e}'
                print(msg)
                error['error'].append(msg)
            except Exception as e:
                print(f'Exception: {str(item["path"])} \n{e}')
                raise

            # read content and add it to the tally
            prev_val = self.read_bytes.value
            try:
                hash_func = hashlib.new(self.algorithm)
                if fd:
                    while read_to_hash(hash_func, fd, self.read_bytes, 1024*1024): pass

                    hex_digest = hash_func.digest().hex().lower()

            except (IOError, OSError) as e:
                msg = f'Error reading data: {str(item["path"])} \n{e}'
                print(msg)
                error['error'].append(msg)

            except Exception as e:
                print(f'Exception: {str(item["path"])} \n{e}')
                raise
            
            # Close file
            if fd: fd.close()

            # Check file didnt change size in the inbetween
            if self.read_bytes.value != prev_val + item['size']:
                msg = f'File size changed from {item["size"]} to {self.read_bytes.value - prev_val}: {str(item["path"])}'
                print(msg)
                error['error'].append(msg)
                item['oldsize'] = item['size']
                item['size'] = item['path'].stat().st_size
            

            # Update item with the new values
            if error['error'] and item:
                item.update(error)
            
            # Update hash
            if item:
                item.update({
                    'hash': hex_digest,
                    'hash_algorithm': self.algorithm,
                     })
            
            # add item to buffer
            if item:
                output_buffer.append(item)
        
        # Send items back
        self.out_queue.put(output_buffer)


class AsyncSpawner(Thread):
    """
    Spawn a QueuedFileHasher_mp() in a separate thread because spawning processes takes a long time on windows
    
    AsyncSpawner.done will become True once its done spawning the process.
    The worker process can be retrieves on AsyncSpawner.worker
    """

    def __init__(self, files, common_queue_pos, algorithm, name):
        super().__init__(target=self.spawner, args=[files, common_queue_pos, algorithm, name])

        self.done = False
        self.worker = None

        self.start()

    def spawner(self, work_queue, work_queue_pos, algorithm, name):
        self.worker = QueuedFileHasher_mp(
            work_queue,
            work_queue_pos, 
            algorithm=algorithm, 
            name=name
            )

        self.done = True


def hash_files(files, cpu_threads, algorithm = 'sha1'):
    """
    Create a list with all the hashes corresponding to the files.

    Parameters:
        files:      List of {path, size} entries

    Return:
        List:      [ {hash, path, size}, ... ]
    """
    # Rate limiter for writing progress to console
    rate_limiter = timed_tigger(10)

    # accumulated data read
    acc_size = 0     

    # total data to read
    total_size = sum( [item['size'] for item in files] )   
    
    # Split the list for each thread in a smart way to avoid spawning 12 processes for 12 items
    splits = split_list(files, cpu_threads, 25)
    cpu_threads = min(cpu_threads, len(splits))

    # Progress bar class
    pb = MultiProgressBar(_max = [total_size, cpu_threads], _min = 0, nbars = 2, update_rate = (1/20), lenght = 35, ignore_over_under= True, charset = "#-", autostart = True)
    pb.pretext = "\033[2K\r"

    pb.set_endtext(" Hashing files...")
    pb.bars_indicator = 0

    process_pool = []
    output = []
    while len(output) < len(files):
        # Loop until we get back all the jobs

        # Spawn the worker processes
        if len(process_pool) < cpu_threads:
            counter = Value('i', 0)
            process_pool.append({'spawner': AsyncSpawner(splits[len(process_pool)], counter, algorithm='sha1', name=f'proc-{len(process_pool)}'),
                                 'position': counter})
            pb.set(1, len(process_pool))
        
        # Check if the spawner finished and add a key for the worker if so
        for i, proc in enumerate(process_pool):
            if 'worker' in proc:
                try:
                    tmp = proc['worker'].out_queue.get(timeout=0.1)
                    output.extend(tmp)
                except queue.Empty as e:
                    pass
            else:
                if proc['spawner'].done:
                    proc['worker'] = proc['spawner'].worker
                    proc['spawner'].join()
                    del(proc['spawner'])
            
        
        if rate_limiter.triggered():
            # Update progress bar
            acc_size = 0

            for proc in process_pool:
                try:
                    acc_size += proc['worker'].read_bytes.value
                except:
                    pass

            pb.set(0,acc_size)
    
    # Set the progress bar to max
    pb.set(0, total_size)
    pb.set_endtext(" Finishing tasks")
    

    # Stop all workers and join them
    for process in process_pool:
        process['worker'].stop()
    
    while len(process_pool):
        process_pool[0]['worker'].join()
        del( process_pool[0] )
    
    # Stop progress bar
    pb.set_endtext(" Done")
    pb.stop(True); del pb    
    
    return output


            