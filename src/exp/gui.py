# Copyright (c) 2011, Peter Thatcher
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   1. Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#   2. Redistributions in binary form must reproduce the above copyright notice,
#      this list of conditions and the following disclaimer in the documentation
#      and/or other materials provided with the distribution.
#   3. The name of the author may not be used to endorse or promote products
#      derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
# OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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
