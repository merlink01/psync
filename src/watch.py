from thirdparty import pyinotify

# *** Wow.  watching ~ uses A LOT of CPU!

class Handler(pyinotify.ProcessEvent):
    def process_default(self, event):
        print ("default", event)

    # also process_IN_CREATE and process_IN_DELETE
    def process_IN_MODIFY(self, event):
        print ("IN_MODIFY", event.path, event.name)

    def process_IN_CREATE(self, event):
        print ("IN_CREATE", event.path, event.name)

    def process_IN_DELETE(self, event):
        print ("IN_DELETE", event.path, event.name)

def on_loop(notifier):
    print ("on_loop", notifier)

if __name__ == "__main__":
    import sys
    root = sys.argv[1]

    # pyinotify.log.setLevel(10)
    handler = Handler()
    
    # Exclude patterns from list
    excl_lst = []  # ['(^|*/).bibble']
    excl = pyinotify.ExcludeFilter(excl_lst)

    wm = pyinotify.WatchManager()
    # first arg can be a list
    # can use pyinotify.ALL_EVENTS
    # rec=True means recursive.  Must have!
    wm.add_watch(root, pyinotify.IN_MODIFY | pyinotify.IN_CREATE | pyinotify.IN_DELETE,
                 rec=True, auto_add=True, exclude_filter=excl)
    notifier = pyinotify.Notifier(wm, default_proc_fun=handler)
    notifier.loop(callback = on_loop)
    # if daemonize = True, spawns another process
    # notifier.loop(daemonize=True, callback=on_loop,
    #              pid_file='/tmp/pyinotify.pid', stdout='/tmp/stdout.txt')
