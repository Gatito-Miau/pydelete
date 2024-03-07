#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# MultiProgressBar
# Version: 1.0.0b

import sys, threading, platform, time
import python_utils.terminal

# Deps: python -m pip install <library>
# python -m pip install python_utils

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class _witness():

    PRESET1 = "|/-\\|/-\\" # Classic spinner
    PRESET2 = "←↖↑↗→↘↓↙"
    PRESET3 = "▁▂▃▄▅▆▇█▇▆▅▄▃▁"
    PRESET4 = "▉▊▋▌▍▎▏ ▏▎▍▌▋▊▉"
    PRESET5 = "▖▘▝▗"
    PRESET6 = "┤┘┴└├┌┬┐"
    PRESET7 = "◢◣◤◥"
    PRESET8 = "◰◳◲◱"
    PRESET9 = "◴◷◶◵"
    PRESET10 = "◐◓◑◒"
    PRESET11 = "⣾⣽⣻⢿⡿⣟⣯⣷"
    PRESET12 = "⠁⠂⠄⡀⢀⠠⠐⠈"

    def __init__(self, speed = 0.5, charset = "" ):
        self.speed = speed
        self.charset = charset

        self.i = 0

        self.local = threading.local()
        self.properties_lock = threading.RLock()

        self.repeater = repeater(speed, self.spin, True)

    def get_char(self):
        self.properties_lock.acquire()
        try:
            self.local.s = self.charset[self.i]
        except:
            self.i = 0
            self.local.s = self.charset[self.i]

        self.properties_lock.release()

        return self.local.s

    def spin(self):
        self.properties_lock.acquire()

        if len(self.charset) < 1:
            self.i = 0
        else:
            self.i = (self.i + 1) % len(self.charset)

        self.properties_lock.release()


class repeater():
    def __init__(self, speed = 0.1, function = None, autostart = False):
        self.speed = speed
        self.function = function

        self.timer = threading.Thread(target=self.repeat)
        self.timer.daemon = True

        self.timer_lock = threading.RLock()
        self.timer_run = threading.Event()

        self.exit = threading.Event()


        self.timer.start()
        if autostart: self.timer_run.set()

    def __del__(self):
        self.exit.set()
        self.timer_run.set()

    def repeat(self):
        while not self.exit.is_set():
            time.sleep(self.speed)
            if self.timer_run.is_set(): self.function()
            self.timer_run.wait()

    def start(self):
        self.timer_run.set()

    def stop(self):
        self.timer_run.clear()



class MultiProgressBar():
    def __init__(self, _max = 100, _min = 0, nbars = 5, update_rate = 0.1, lenght = 10, ignore_over_under = False, autostart = False, charset = "#=~-.", terminal_width=None):
        self.update_rate = update_rate
        self.ignore_over_under = ignore_over_under
        self.show_percentage = True
        self.terminal_width = terminal_width
        
        if type(_max) is int:
            self.bars_max = [_max for i in range(nbars)]
        else:
            self.bars_max = [_max[i] for i in range(nbars)]
        
        if type(_min) is int:
            self.bars = [_min for i in range(nbars)]
            self.bars_min = [_min for i in range(nbars)]
        else:
            self.bars = [_min[i] for i in range(nbars)]
            self.bars_min = [_min[i] for i in range(nbars)]
            
        
        self.bars_color = ["" for i in range(nbars)]
        self.bars_lenght = lenght
        self.bars_indicator = 0 # This selects which bar is the main progress bar
        
        self.bars_chrs = charset

        self.endtext = " "
        self.pretext = ""

        self.properties_lock = threading.RLock()

        self.local = threading.local()

        self.timer = None
        self.print_lock = threading.RLock()

        self.spinner = _witness(0.1,_witness.PRESET11)
#         if platform.system() == "Windows":
#             self.spinner.charset = _witness.PRESET1

        if autostart: self.start(True)

    def gen_bar(self):
        self.properties_lock.acquire()
        ret = ""

        # -- Generate Bars --
        n = min(len(self.bars), len(self.bars_chrs))
        s = ""
        a = 0
        for i in range(n):
            if self.bars_max[i] == 0:
                c = 0
            else:
                c = round( (self.bars[i] / self.bars_max[i]) * self.bars_lenght)
            c = max(c, 0)
            c = min(c, self.bars_lenght)

            v = max(c, a)

            if platform.system() != "Windows": s += self.bars_color[i]
            s += self.bars_chrs[i] * (v - a)

            a += (v - a)

        if platform.system() != "Windows": s += bcolors.ENDC
        s += " "* (self.bars_lenght - a)

        # -- Calculate progress --
        if self.show_percentage:
            if self.bars_max[self.bars_indicator] == 0:
                b = 0.0
            else:
                b = (self.bars[self.bars_indicator] / self.bars_max[self.bars_indicator]) * 100
            p = ("%6.6f%%" % (b))[:6]
        else:
            p = "%d/%d" % ( self.bars[self.bars_indicator], self.bars_max[self.bars_indicator] )

        ret =  "%s[%s] %s %s%s" % (self.pretext, s, p, self.spinner.get_char(), self.endtext)
        
        # -- Cut so it wont overflow the terminal and clear the rest --
        if not self.terminal_width:
            w, h = python_utils.terminal.get_terminal_size()
            ret += " "*(w-len(ret))
            ret = ret[:w]
        else:
            ret += " "*(self.terminal_width-len(ret))
            ret = ret[:self.terminal_width]
        
        
        self.properties_lock.release()

        return ret

    def print_bar(self):
        self.properties_lock.acquire()
        self.local.print_bar_b = ("\r%s" % self.gen_bar()).encode("utf-8", errors='surrogateescape')

        try:
            sys.stdout.write(self.local.print_bar_b.decode(sys.stdout.encoding, errors='surrogateescape'))
        except:
            sys.stdout.write(self.local.print_bar_b.decode(sys.stdout.encoding, errors='replace'))

        self.properties_lock.release()

    def set(self, n, value):
        self.properties_lock.acquire()
        self.bars[n] = value
        self.properties_lock.release()

    def get(self, n):
        self.properties_lock.acquire()
        self.local.value = self.bars[n]
        self.properties_lock.release()

        return self.local.value

    def set_max(self, n, value):
        self.properties_lock.acquire()
        self.bars_max[n] = value
        self.properties_lock.release()

    def set_max_all(self, value):
        self.properties_lock.acquire()
        for i in range(len(self.bars_max)):
            self.bars_max[i] = value
        self.properties_lock.release()

    def set_min(self, n, value):
        self.properties_lock.acquire()
        self.bars_min[n] = value
        self.properties_lock.release()

    def set_min_all(self, value):
        self.properties_lock.acquire()
        for i in range(len(self.bars_min)):
            self.bars_min[i] = value
        self.properties_lock.release()

    def set_endtext(self, s):
        self.properties_lock.acquire()
        self.endtext = s
        self.properties_lock.release()

    def start(self, now=False):
        self.properties_lock.acquire()


        if self.timer == None:
            self.timer = repeater(self.update_rate, self.print_bar)
            self.timer.start()

        if now: self.print_bar()

        self.properties_lock.release()

    def stop(self, now=False):
        self.properties_lock.acquire()

        if self.timer != None:
            self.timer.stop()

            del self.timer
            self.timer = None

        if now: self.print_bar()

        self.properties_lock.release()

    def reset(self):
        self.properties_lock.acquire()

        for i in range(len(self.bars)):
            self.bars[i] = self.bars_min[i]

        self.stop()

        self.properties_lock.release()


if __name__ == "__main__":
#     class MultiProgressBar():
#     def __init__(self, _max = 100, _min = 0, nbars = 5, update_rate = 0.1, lenght = 10, ignore_over_under = False, autostart = False, charset = "#=~-."):
    
    _spin = [_witness.PRESET1, _witness.PRESET12, _witness.PRESET5, _witness.PRESET10, _witness.PRESET11, ]
    _max = 3343
    
    params = {"_max": _max, "_min": 0, "nbars": 5, "update_rate": (1/60), "lenght": 35, "ignore_over_under": False, "autostart": True, "charset": '#=~-.'}
    pb = MultiProgressBar(**params)
    
    
    
    for i in range(params['nbars']):
        bar_index = params['nbars']-i-1
        
        
        pb.bars_indicator = bar_index
        
        pb.spinner.charset = _spin[i]
        pb.endtext = ' Test %i - %s' % (i+1, _spin[i]) + ' '*8
        
        for v in range(pb.bars_max[bar_index]):
            pb.set(bar_index, v+1)
            
            time.sleep( (1/_max)/4 )
        
        pb.set(bar_index, _max)
    
    pb.endtext = ' Tests Finished!'+' '*8
    pb.stop(True); del(pb)
    