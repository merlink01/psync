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

    import code
    import sys
    import time

    from xml.etree import cElementTree as ET
    from thirdparty import sleekxmpp

    FILES_NS = "{latus}files"

    class XmppClient:
        def __init__(self, jid, password, server_address):
            self.server_address = server_address
            self.client = sleekxmpp.ClientXMPP(jid, password)
            for event, handler in ((attr[3:], getattr(self, attr))
                                   for attr in dir(self)
                                   if attr.startswith("on_")):
                self.client.add_event_handler(event, handler)
            self.jid = None
            
            self.client.register_handler(
                sleekxmpp.Callback("Latus files IQ",
                    sleekxmpp.MatchXPath("{jabber:client}iq/%s" % (FILES_NS,)),
                    self.on_files_iq))

        def run(self):
            if self.client.connect(self.server_address):
                self.client.process(threaded=False)
            else:
                raise Exception("failed to connect")            

        def on_connected(self, _):
            print ("connected", )

        def on_failed_auth(self, _):
            print ("failed auth", )
            # TODO: make it stop :)

        def on_session_start(self, _):
            print ("session start", self.client.boundjid.full)
            self.client.get_roster()
            # print ("got roster", self.client.roster)
            ## If you don't do this, you wan't appear online.
            self.client.send_presence()
            print ("address", self.client.address)
            self.jid = self.client.boundjid.full

        def on_disconnected(self, _):
            print ("disconnected", )

        def on_got_online(self, presence):
            remote_jid = presence["from"].full
            if remote_jid.endswith("6c617473"):
                print (self.jid, "online", remote_jid)

        def on_got_offline(self, presence):
            remote_jid = presence["from"].full
            if remote_jid.endswith("6c617473"):
                print (self.jid, "offline", remote_jid)

        def on_files_iq(self, iq):
            print (self.jid, "files iq", iq.xml[0])

        def send_get_files(self, tojid, since):
            iq = self.client.make_iq()
            iq["to"] = tojid
            iq["type"] = "get"
            iq["id"] = "TODO"  # ***
            iq.append(ET.Element(FILES_NS, {"since": str(1234)}))
            iq.send()
            # *** return Future for the parsed response
            

    password = sys.argv[1]
    server_address = ("talk.google.com", 5222)

    client1 = XmppClient("pthatcher@gmail.com/test2_6c617473",
                         password, server_address)

    client2 = XmppClient("pthatcher@gmail.com/test1_6c617473",
                         password, server_address)
    
    start_thread(client1.run, "XmppClient 1")
    start_thread(client2.run, "XmppClient 2")
    time.sleep(1.0)
    client1.send_get_files(client2.jid, 1234)

    code.interact("Debug Console", local = {
        "client1": client1,
        "client2": client2})
        
    # notes:
    # for gmail, the last 8 lettes must be hex.  If they are, you can
    # be as long as you want.  If they are not, you have to be only 10
    # chars.  So, basically you get 10 chars, and have to have 8 hex
    # chars after that.
    #
    # connected({}) fires when connected to server (not authed)
    # disconnected({}) fires when disconnected from server
    # failed_auth({}) fires when auth is wrong
    # got_online and got_offline are useful
    # presence has (from .getStanzaValues)
    #   status, from, show, priority, to, type, id
    #   from and type seem useful.
    #     type can be "available" and "away"
    # client.roster is
    #    {name: {name: ...,
    #            presence: {status: '',
    #                       priority: 24,  # ???
    #                       show: 'available'},
    #            in_roster: True,
    #            groups: [],
    #            subscription: both}}
    # client seems to have:
    #   .send_xml(xml)
    #   .send_message(to, body,
    #   .make_iq_result(id)
    #   .make_iq_get()
    #   .make_iq_set()
    #     then do iq.append(?)
    #  
