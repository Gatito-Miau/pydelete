#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Version: 1.0.0b
# Usage: pydelete.py <folder>

# Deps: python -m pip install pywin32

import sys, os, time, signal, math, stat, itertools, argparse, json, datetime, time
import multiprocessing, threading, requests, queue, platform, unicodedata

from pathlib import Path

from pydelete_utils     import *
from multiprogressbar   import *
from fileshasher        import *

# Options
# place_synlink = False
# check_for_folders = False # Beta, barely working.
# add_rm_original = True
# use_absolute_paths = True
# base_directory = ''

# Variables
HASH_ALGORITHM = "sha1"
cpu_threads    = os.cpu_count()

def get_file_pos(path):
    """
    Return a number to represent the position of a file in a disk

    Parameters:
        path:        path to the file
    
    return:
        Dict        {pos}
    """
    if os.name == 'nt':
        return { 'pos': get_file_LCN(path)['LCNn'] } 
    else:
        return { 'pos': Path(path).stat().st_ino } 

def get_file_LCN(path):
    """
    Get file's LCN number 

    path:        path to the file
    
    return:     {LCNn}
    """

    return { 'LCNn': GET_RETRIEVAL_POINTERS(path)[3] }

def dir_scan(path, recusive = True, symlinks = True, abs = False, file_callback = None, progress_callback = None):
    """
    Scan directory recursively

    path:       Path to the directory or file (str)
    recusive:   Whether to scan recursively or not (bool)
    symlinks:   Follow symlinks
    abs:        Return absolute paths instead or relative ones
    file_callback:      List of function(filepath) to call for each file. Return type should be dict or None.
                        If its dict the internal item will be updated with it
    progress_callback:  List of function(current_pos, total_files_count, file_path). Gets called for each file.
                        Return value gets ignored.

    return:     [ {path, size}, ... ]
    """
    
    if isinstance(path, list):
        path = [Path(i) for i in path]
    else:
        path = [Path(path)]
    
    if file_callback:
        if not isinstance(file_callback, list):
            file_callback = [file_callback]
    
    if progress_callback:
        if not isinstance(progress_callback, list):
            progress_callback = [progress_callback]



    # Create internal list
    files = []
    for i,v in enumerate(path):
        files.append({
            'path': v,
            'size': v.stat().st_size
            })
    
    i = 0
    while i < len(files): # Scan dir recusively and its files and makes a list
        # Do progress callbacks
        if progress_callback:
            for func in progress_callback:
                func(i, len(files), str(files[i]['path']) )
                
        # Ignore symlinks
        if files[i]['path'].is_symlink() and not symlinks:
            del(files[i]); continue
        
        # Convert path to absolute
        if abs:
            files[i]['path'] = files[i]['path'].absolute()
        
        
        # Scan subdirectories
        if files[i]['path'].is_dir():
            try:
                tmp = []
                for item in files[i]['path'].iterdir():
                    if item.is_dir() and not recusive:
                        continue
                    else:
                        try:
                            tmp.append({
                                'path': item,
                                'size': 0 if item.is_dir() else item.stat().st_size
                                })
                        except Exception as e:
                            print(f'\nError reading files. Skipping {e}')
                
                files[i:i+1] = tmp
            
            except KeyboardInterrupt: raise
            except Exception as e:
                print(f'Error reading files. {e}')
    
            continue

        if file_callback:
            for func in file_callback:
                r = func( str(files[i]['path']) )
                if r:
                    files[i].update( r )

        i += 1
    
    return files

def check_for_repeated_files(files: list, cpu_threads: int = 1):
    """
    Check for reapeated hashes in the files list and generate a list with all the repeated files per hash

    Parameters:
        files:      [ {path, size, hash}, ... ]
    
    Return:
        dict::      { hash: {[files], size}, ... }
    
    input must be sorted by hash, aka all paths with the same hash together, one after the other, not necesarily in alphabetical order.
    """
    # TODO: refactor this for readability, this is old code.

    _rep = {}
    _count = 0
    _acc_size = 0
    
    pb = MultiProgressBar(_max = len(files), _min = 0, nbars = 2, update_rate = (1/20), lenght = 35, ignore_over_under= True, charset = "#-", autostart = True)
    pb.pretext = "\033[2K\r"
    
    # Sort entries by hash
    pb.set_endtext(" Sorting list...")
    
    files = sorted(files, key = lambda x: x['hash'])
    
    # Check for repeated hashes
    pb.bars_indicator = 0
    
    for i,v in enumerate(files):
        pb.set(0, i)
        # Initialize hash key if not present
        if v['hash'] not in _rep:
            # Skip not repeated files
            try:
                if ( files[i]['hash'] != files[i+1]['hash'] ): continue
            except: continue
                    
            _rep[v['hash']] = dict(v)
            del( _rep[v['hash']] ['hash'] )
            del( _rep[v['hash']] ['path'] )
            _rep[v['hash']]['files'] = []
        assert( v['hash'] in _rep )
        
        # Check for hash colitions
        assert ( _rep[v['hash']]['size'] == v['size'] ), 'hash colition detected'
        
        # Add path to the coresponding hash's paths list
        _rep[v['hash']]['files'].append( v['path'] )
        
        _acc_size += _rep[v['hash']]['size']
                
        pb.set_endtext(" %d Files. (%s)" % ( len(_rep), human_readable_size(_acc_size) ))
        
    
    # Make sure we dont have non-repeated files in the dict
    for i,v in enumerate(_rep.keys()):
        assert( len(_rep[v]['files']) > 1 )

    pb.set_endtext(" %d Files. (%s)" % ( len(_rep), human_readable_size(_acc_size) ))

    pb.set(0, len(files))
    pb.stop(True); del pb
    
    return _rep


def sort_repeated_files_list(hashes_list):
    """
    Sort repeated files by shortest path first, shortest name second.

    Parameters:
        hashes_list:    { hash: {[files]}, ... }
    
    Return:
        list:            hashes_list but sorted like this [ {hash: {[files], ...}, ... ]
    """
    
    hashes_list = [{i[0]: i[1]} for i in sorted(hashes_list.items(), key=lambda x: x[0])]

    for entry in hashes_list:
        k = tuple(entry.items())[0][0] # dictionary key
        entry[k]['files'] = list(sorted( entry[k]['files'], key=lambda x: (len(Path(x).parts), len(str(x))) ))

    return hashes_list

def write_batch_file(
    repeated_files: list, 
    all_files: list, 
    output_directory: str = '.',
    batch_script_name: str = 'list.sh',
    relative_path: str = '.',
    link_repeated_files: bool = False
):
    """
    Write a batch script to remove duplicate files based on a given list of repeated files.
    
    Parameters:
        repeated_files ([{hash: {path, size, [files]}}, ...]): List of tuples containing file repetitions.
        all_files ([path, ...]):    List of all files in the current directory.
        output_directory (str):     Path to the output directory where the batch script will be saved to.
        batch_script_name (str):    Name of the batch script file (default: 'list.sh').
        relative_path (str):        Relative path from the current directory.
        link_repeated_files (bool): Replace files with hardlinks/symlinks instead of deleting
    
    Returns:
        None
    """
    # Note \\?\C:\

    # Header for the batch script
    header = ''

    # Calculate total number of files and repeated files
    all_files_num = len(all_files)
    repeated_files_num = sum([len(item[tuple(item.keys())[0]]['files'])-1 for item in repeated_files])
    repeated_files_num_size = sum([ item[tuple(item.keys())[0]]['size'] * len(item[tuple(item.keys())[0]]['files'][1:]) for item in repeated_files])

    # Determine command line arguments based on the operating system
    if os.name == 'nt':
        remove_cmd = 'del /F "{}"'
        symlink_cmd = 'mklink "{}" "{}"' # Link target
        hardlink_cmd = 'mklink /H "{}" "{}"' # Link target
        comment_preffix = 'REM'

    else:
        remove_cmd = 'rm -fv "{}"'
        symlink_cmd = 'ln -sv "{}" "{}"' # Target Link
        hardlink_cmd = 'ln -v "{}" "{}"' # Target Link
        comment_preffix = '#'

        # Add shebang line to the header for Unix-based systems
        header += f'#!/bin/sh\n\n'
    
    # Add script header and comments to the batch script
    header += f'{comment_preffix} {batch_script_name}\n\n'
    header += f'cd "{Path(output_directory).absolute()}"\n\n' 
    header += f"{comment_preffix} ---- Repeated files list - {all_files_num} files / {repeated_files_num} repeated ({human_readable_size(repeated_files_num_size)})---- \n\n"

    # Create the main script for removing duplicate files
    main_script = ''
    for item in repeated_files:
        k = tuple(item.keys())[0]
        filename        = Path(item[k]["files"][0])
        

        # Add comment and commands to remove duplicate file
        ref_is_linked = ''
        if filename.is_symlink():
            ref_is_linked = '(symlink)'
        elif filename.stat().st_nlink > 1:
            ref_is_linked = f'(hardlink, {filename.stat().st_nlink})'

        main_script += f'{comment_preffix} {item[k]["hash_algorithm"]}: {k} - "{filename.name}" - {human_readable_size(item[k]["size"])} {ref_is_linked} - {len(item[k]["files"][1:])} repeated files\n' 
        
        main_script += f'{comment_preffix} {remove_cmd.format(item[k]["files"][0])}\n'

        # Add commands to remove additional duplicates for each repeated file
        for file in item[k]["files"][1:]:
            # Add notice if the file is a link
            if Path(file).is_symlink():
                main_script += f'{comment_preffix} The following file is a symlink\n'
            elif Path(file).stat().st_nlink > 1:
                main_script += f'{comment_preffix} The following file is a hardlink ({Path(file).stat().st_nlink})\n'

            main_script += f'{remove_cmd.format(file)}\n'
            
            # If link_repeated_files add a hard link after deleting the repeated file. Use a symlink instead of its on a different drive.
            if link_repeated_files:
                ref_file_dev_id = filename.stat().st_dev
                file_dev_id     = Path(file).stat().st_dev

                link_cmd = hardlink_cmd if ref_file_dev_id == file_dev_id else symlink_cmd

                if os.name == 'nt': # Link, target
                    main_script += f'{link_cmd.format(file, item[k]["files"][0])}\n'
                else: # Target, Link
                    main_script += f'{link_cmd.format(item[k]["files"][0], file)}\n'

        
        main_script += '\n\n'

    # Write the batch script to a file
    with open(batch_script_name, 'w') as f:
        f.write(escape_string_in_utf8(header + main_script))

        
# =============================================================================
# ---- Misc -------------------------------------------------------------------

def sigint_handler(signum, frame):
    print('\nInterrupted')
    # os.kill(os.getpid(), signal.SIGKILL)
    for proc in multiprocessing.active_children():
        print(proc.name, 'stop')
        proc.stop()

    for proc in multiprocessing.active_children():
        proc.join(timeout=0.33)
        print(proc.name, 'joined')
        os.kill(proc.pid, signal.SIGTERM)
    os.kill(os.getpid(), signal.SIGTERM)
    
    exit()

def parse_arguments():
    parser = argparse.ArgumentParser(description='Finds repeated files and makes a batch script to delete them')
    parser.add_argument('path', type=str, help='path to scan.',  nargs='+', default=None)
    
    try:
        args = parser.parse_args()
       
    except argparse.ArgumentError as e:
        print(f'ERROR: {e.message}\n', file=sys.stderr)
        parser.print_help()
        exit(2)
    
    if not args.path:
        parser.print_help()
        exit(1)

    return args

def main(argv):
    
    start_time = time.time()

    paths = [Path(i) for i in args.path]
    for path in paths:
        if not path.is_dir():
            print( f'"{path}" is not a directory')
            return 2
        
    if (len(paths) > 1): use_absolute_paths = True
    
    # ------------------ Scan ----------------------
        
    files = []
    _total_size = 0
    for i, path in enumerate(paths):
        print (f'Scanning: {path}')
        tmp = dir_scan(path, symlinks = True, abs = True, file_callback=get_file_pos, progress_callback = lambda x,y,z: print(f'\r{x}/{y}', end=''))
        
        for f in tmp: _total_size += f['size']
        
        files.extend(tmp)
    
    print ('\r', end='')
    print ('Found %d files (%s)' % (len(files), human_readable_size(_total_size))  )

    # Sort files by LCN/inode number to improve sequential reading on HDDs
    files = sorted(files, key= lambda x: (x['pos'], x['path']))

    # ------------------ Hash -----------------------
    print ('Calculating checksum')

    old = files[:]
    files = hash_files(files, cpu_threads); print()
    files = sorted(files, key= lambda x: (x['pos'], x['path']))

    # Redundant checks because multiprocessing is super buggy
    assert(len(old) == len(files))
    for i in range(len(files)):
        assert(files[i]['path'] == old[i]['path'])
        assert(files[i]['size'] == old[i]['size'])
        assert('hash' in files[i])

    # ----------------- Check ----------------------
    print ('Checking for repeated files')
    files = sorted(files, key= lambda x: x['hash'])

    repeated_files = check_for_repeated_files(files); print()
    # dump_to_json("dump_rep.txt", repeated_files)
    
    repeated_files = sort_repeated_files_list(repeated_files)
    # dump_to_json("dump_batch.txt", repeated_files)

    # ------ Batch - Write commands to file ---------
    script_name = 'replist' + '.bat' if os.name == 'nt' else '.sh'

    if (len(repeated_files) > 0):
        print (f'Creating {script_name} at', os.getcwd())

        write_batch_file(repeated_files, files, '.', script_name , '.', link_repeated_files=False)

    else:
        print (f'No repeated files.')
    
    
    # start_time = datetime.datetime.now()
    finish_time = time.time()
    
    # ----------------------------------------------
    print (f'Finished in { datetime.timedelta( seconds=(finish_time-start_time)//1 )}')

 
if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint_handler)
    
    args = parse_arguments()
    main(args)
