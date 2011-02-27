import sleekxmpp

# TODO:
#  listen for presence of others, and create a PeerServer to handle messages
#    or delete a PeerServer when the peer goes away
#  listen for messages and route them to the correct PeerServer
#  listen for our own disconnect and try and reconnect

class XmppHandler:
    def on_start(self, event):
        client.getRoster()
        client.sendPresence()

        print ("start roster",)
        for name, details in client.roster.viewitems():
            print "{0:30}: {1[name]}, {1[presence]}".format(name, details)
    

if __name__ == "__main__"
    import sys
    password = sys.argv[0]

    jid = "pthatcher@gmail.com/02044160"
    # hack for typing password
    password = "".join(chr(ord(c)+1) for c in password)

    client = sleekxmpp.ClientXMPP(jid, password)
    # client.getRoster fetches the roster, which is then in client.roster
    # as {name: {name: ..., presence: ...}}
    # client.sendPresence sends our presence (should be done at start

    def on_start(event):

    client.add_event_handler("session_start", on_start)

    def on_changed_status(presence):
      print repr(presence)

    client.add_event_handler("on_changed_status", on_message)

    def on_message(msg):
      # check type
      # can do msg.reply().send() ?
      print repr(msg)

    client.add_event_handler("message", on_message)

    # Other events: connected, changed_status(presence), disconnected,
    # failed_auth, got_online(presence), got_offline(presence),
    # on_message, presence_available, presence_error,
    # presence_unavailable, roster_update, session_start

    if client.connect(("talk.google.com", 5222)):
      print "logged in"
      client.process(threaded=False)
      # client has:
      # .address (server address)
      # .boundjid
      #   .bare
      #   .full
      # .roster
    else:
      print "Couldn't connect"
