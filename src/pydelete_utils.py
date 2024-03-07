import time, os, math

from pathlib import Path

import win32file, winioctlcon, struct

class timed_tigger():
    """A utility class that can be used to trigger an event at a specified rate."""

    def __init__(self, rate):
        """Initializes the trigger with a given rate.
        
        Parameters:
            rate (float): The rate at which events should occur. Events per second.
            
        Attributes:
            last_time (float): The time when the event was last triggered.
            interval (float): The minimum time between triggers, calculated from the given rate.
        """

        self.last_time = time.time()
        self.interval = (1.0/rate)
        
    def triggered(self, reset = True):
        """Checks if an event should be triggered based on the set rate.
        
        Parameters:
            reset (bool): If True, resets the last_time attribute to current time when the function is called. Defaults to True.
            
        Returns:
            bool: Whether or not an event should occur."""

        if time.time() > ( self.last_time + self.interval ):
            if reset: self.last_time = time.time()
            return True 
        return False 

def split_list(items, num_of_splits, min_per_split):
    """
    Splits a list into N sublists where each sublist has at least M elements.
    
    Parameters:
        items (list): The list to be split.
        num_of_splits (int): The number of desired splits. The function will decrease this number if necessary 
                             to ensure that each split has a minimum of min_per_split elements.
        min_per_split (int): The minimum number of elements per split.
    
    Returns:
        list: A list of sublists, where each sublist is a split from the original list.
    """
    
    # Decrease num_of_splits if necessary to ensure that each split has at least min_per_split elements
    while num_of_splits > 0 and num_of_splits * min_per_split > len(items):
        num_of_splits -= 1
    
    # Ensure num_of_splits is always at least 1
    num_of_splits = max(num_of_splits, 1)

    splits = []
    
    for n in range(num_of_splits):
        # For each split, create a list of items whose indices are congruent to the current split index (n) modulo num_of_splits
        splits.append([items[i] for i in range(n, len(items), num_of_splits)])
    
    return splits

def GET_RETRIEVAL_POINTERS(path):
    """
    GET_RETRIEVAL_POINTERS(path)
    Returns [ExtentCount, StartingVcn, *Extents[ExtentCount]], Extents = [NextVcn, Lcn]
    The values of NextVcn, Lcn and StartingVcn are in # of clusters.
    
    https://docs.microsoft.com/en-us/windows/win32/fileio/clusters-and-extents
    Clusters may be referred to from two different perspectives: within the file and on the volume.
    Any cluster in a file has a virtual cluster number (VCN), which is its relative offset from the beginning of the file.
    For example, a seek to twice the size of a cluster, followed by a read, will return data beginning at the third VCN.
    
    An extent is a run of contiguous clusters.
    For example, suppose a file consisting of 30 clusters is recorded in two extents.
    The first extent might consist of five contiguous clusters, the other of the remaining 25 clusters.
    
    A logical cluster number (LCN) describes the offset of a cluster from some arbitrary point within the volume.
    LCNs should be treated only as ordinal, or relative, numbers.
    There is no guaranteed mapping of logical clusters to physical hard disk drive sectors.
    There is no guarantee of any relationship on the disk of any extent to any other extent.
    For example, the first extent may be at a higher LCN than a subsequent extent.
    """

#     C:\Program Files\Python38\Lib\site-packages\PyWin32.chm

#     http://www.disk-space-guide.com/ntfs-disk-space.aspx
#     https://web.archive.org/web/20060101061522/http://www.wd-3.com/archive/luserland.htm
#     https://docs.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_get_retrieval_pointers
#     https://docs.microsoft.com/en-us/windows/win32/api/winioctl/ns-winioctl-retrieval_pointers_buffer
#     typedef struct RETRIEVAL_POINTERS_BUFFER {
#       DWORD                    ExtentCount;
#       LARGE_INTEGER            StartingVcn;
#       struct {
#         LARGE_INTEGER NextVcn;
#         LARGE_INTEGER Lcn;
#       };
#       __unnamed_struct_087a_54 Extents[1];
#     } RETRIEVAL_POINTERS_BUFFER, *PRETRIEVAL_POINTERS_BUFFER;
    
    StartingVcn = struct.pack("Q", 0)
    
    in_buf_size = 8
    extents     = 1
    
    # GetDiskFreeSpace(rootPath)
    # [sectors per cluster, bytes per sector, total free clusters on the disk, total clusters on the disk]
    path = os.path.abspath(path)
    
    DiskFreeSpace = win32file.GetDiskFreeSpace(os.path.splitdrive(path)[0])
    BytesPerCluster = DiskFreeSpace[0] * DiskFreeSpace[1]
    
    DesiredAccess       = win32file.GENERIC_READ
    ShareMode           = win32file.FILE_SHARE_READ
    CreationDisposition = win32file.OPEN_EXISTING
    hHandle = win32file.CreateFileW(path, DesiredAccess, ShareMode, None, CreationDisposition, 0, None)
    
    raw_data = None
    while(not raw_data):
        try:
            raw_data = win32file.DeviceIoControl( hHandle, winioctlcon.FSCTL_GET_RETRIEVAL_POINTERS, StartingVcn, 16+(8*2*extents) )
        
        except Exception as e:
            assert(not raw_data)

            # pywintypes.error: (122, 'DeviceIoControl', 'El área de datos transferida a una llamada del sistema es demasiado pequeña.')
            if e.args[0] == 122:
                # OutBuffer too small for 1 extent
                extents += 1
                continue

            # pywintypes.error: (38, 'DeviceIoControl', 'Se ha alcanzado el final del archivo.')
            elif e.args[0] == 38:
                # File too small. Resident in MFT. Ordering by disk position doesnt do much for lots of tiny files anyways.
                # So as a quick fix we set them to position 0
                if os.stat(path).st_size > 1024:
                    raise
                raw_data = struct.pack("QQQQ", 1,0,2,0)
            
            # pywintypes.error: (234, 'DeviceIoControl', 'Hay más datos disponibles.')
            elif e.args[0] == 234:
                # OutBuffer too small, more extent available
                extents += 1
                continue
            
            else:
                print(e)
                raise
                
    
    hHandle.close()
      
    assert(len(raw_data) == 16+(8*2*extents)), "%i - %i"%(len(raw_data), 16+(8*2*extents))
    
    RETRIEVAL_POINTERS_BUFFER_FORMAT = "QQ"+"QQ"*extents
    data = list(struct.unpack(RETRIEVAL_POINTERS_BUFFER_FORMAT, raw_data))
    
    # For some reason ExtentCount gets corrupted if there wasnt enough space from the first call
    data[0] = extents
    
    return data

# ---- Functions - Misc -------------------------------------------------------
def dump_to_json(path, obj):
    """
    Dump an object to a json file while converting bytes and Path objects to strings.
    
    Parameters:
        path (str or Path): The file path to save the json dump to.
        obj: The object to convert to json format. This can be of any type, but special handling is done for dictionaries, lists, bytes, and Path objects.
    
    Raises:
        TypeError: If 'path' is not a string or Path object.

    """

    def fix_bytes(obj):
        """
        This function recursively traverses through all elements in an object and checks their type.
        If it is a dictionary, list or bytes instance, it converts them into appropriate formats before returning the converted object. 
        """
        
        import base64  # Import the base64 module for encoding/decoding data to/from Base64 format
        from pathlib import Path  # Import the Path class from the pathlib module, which represents file system paths
        
        if isinstance(obj, dict):
            # If obj is a dictionary, it iterates over all its items. 
            obj = {item[0]: fix_bytes(item[1]) for item in obj.items()}
            return obj

        elif isinstance(obj, list):
            # If obj is a list, it applies fix_bytes function to each of its elements and returns the updated list.
            obj = [fix_bytes(item) for item in obj]
            return obj
    
        elif isinstance(obj, bytes):
            # If obj is a byte instance, it converts them to Base64 format and then decodes the resulting byte string to UTF8. 
            return base64.urlsafe_b64encode(obj).decode('UTF8')

        elif isinstance(obj, Path):
            # If obj is a Path instance, it converts it to a string and returns this string. 
            return str(obj)

        else:
            # If none of the above conditions match, then simply return the original object as it is since there is no need for conversion.
            return obj

    obj = fix_bytes(obj) 
    
    import json  # Import the json module which provides methods to manipulate json data
    with open(path, "w") as f:
        f.write(json.dumps(obj, indent=4))

def human_readable_size(size: int, decimals: int = 2, binary_units: bool = False, long_string: bool = False) -> str:
    """ This function returns a 'human-readable' string to represent file size in bytes. The value n is the number of bytes and can be any real number, not necessarily an integer. This function converts that number into a human readable format with power of 2 or 1024 depending on long_string variable"""
    
    # Make sure size an integer and is not negative
    size = max( 0, int(size) )

    # Determine which format is to be used based on long_string variable
    UNITS_1000 = {False: ["B",  "KB",  "MB",  "GB",  "TB",  "PB",   "EB"], True: ["Bytes", "KiloBytes", "MegaBytes", "GigaBytes", "TeraBytes", "PetaBytes", "ExabBytes"]}
    UNITS_1024 = {False: ["B", "KiB", "MiB", "GiB", "TiB", "PiB",  "EiB"], True: ["Bytes", "KibiBytes", "MibiBytes", "GibiBytes", "TebiBytes", "PebiBytes", "ExbiBytes"]}
    UNITS      = {True: UNITS_1024, False: UNITS_1000}

    # Define the conversion constants.
    conv_constant = {True: 1024, False: 1000}
    
    # Get the exponent value, which is used to determine power of 2 or 1024
    exp = 0 if size == 0 else  math.floor(math.log(size, conv_constant[binary_units]))

    # Format string to be returned
    return f"{size / (conv_constant[binary_units] ** exp):.{decimals}f} {UNITS[binary_units][long_string][exp]}"

def human_readable_datarate(size: int, decimals: int = 2, binary_units: bool = False) -> str:
    """ Convert a number into a string with the proper binary datarate unit """

    # Make sure size an integer and is not negative
    size = max( 0, int(size) )

    UNITS_1000 = ["B/s", "KB/s",  "MB/s",  "GB/s",  "TB/s",  "PB/s",  "EB/s"]
    UNITS_1024 = ["B/s","KiB/s", "MiB/s", "GiB/s", "TiB/s", "PiB/s", "EiB/s"]
    UNITS      = {True: UNITS_1024, False: UNITS_1000}

    exp = 0 if size == 0 else math.floor(math.log(size, 1024))

    return f'{round(size/(1024**exp)):0.{decimals}f} {UNITS[binary_units][exp]}'

def escape_string_in_utf8(text):
    """Escape string and return a valid utf-8 string"""

    while True:
        encoded_text = text.encode("utf-8", errors="surrogateescape")

        try:
            return encoded_text.decode("utf-8", errors="strict")
        
        except UnicodeDecodeError as e:

            text = ''
            text += encoded_text[:e.start].decode("utf-8", errors="strict")

            text += ''.join(f'\\x{b:x}' for b in encoded_text[e.start: e.end])
            
            text += encoded_text[e.end:].decode("utf-8", errors="surrogateescape")

def lsd_radix_sort(iterable, *, key=None):
    """LSD Radix sort function"""
    
    if not iterable:  # If the list is empty, return it as it is.
        return iterable
        
    _max = max(iterable)
    
    # Calculate maximum number of digits in the list
    _max_digits = 0
    while _max > 0:
        _max //= 10  # Integer division
        _max_digits += 1
        
    # bins is the array for each digit
    for x in range(_max_digits):  
        bins = [[] for i in range(10)]  # Initialize an empty list of lists (bins)
        for y in iterable:  # For each item in the list
            index = 0 if x >= len(str(y)) else int(str(y)[::-1][x])  # Get digit at position x, from right to left. If x is out of range for y, use 0.
            bins[index].append(y)  # Append the item to the appropriate bin based on the digit's value.
        iterable = [item for sublist in bins for item in sublist]  # Flatten the bins into a new list of items.
        
    return iterable
