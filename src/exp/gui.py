# import Tix
# f = Tix.Toplevel()	# Tix imports Tkinter so Tk classes
#                         # may be used directly
# w = Tix.LabelEntry(f, label='Xyz:')	# Tix compound widget
# w.label['width'] = 10
# w.entry['bg'] = 'cyan'
# w.text.insert(Tix.END, 'Hello, world')

import Tkinter as Tk
import Tix

root = Tix.Tk()
# setup HList
hl = Tix.HList(root, columns = 5, header = True)
hl.header_create(0, text = "File")
hl.header_create(1, text = "Date")
hl.header_create(1, text = "Size")
# create a multi-column row
hl.add("row1", text = "filename.txt")
hl.item_create(entry_path, 1, text = "2009-03-26 21:07:03")
hl.item_create(entry_path, 2, text = "200MiB")
