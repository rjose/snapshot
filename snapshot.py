# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import io
import os
import subprocess

import zmq

from utils.sectionize import sectionize

context = zmq.Context()

# TODO: Clean up this file with malice

# This is a socket for replying to requests for snapshot data
socket = context.socket(zmq.REP)
socket.bind("tcp://127.0.0.1:5000")

pub_socket = context.socket(zmq.PUB)
pub_socket.bind("tcp://127.0.0.1:5100")

#TODO: Add functions to create stacked streams

while True:
    msg = socket.recv()
    input = io.StringIO(msg)
    sections = sectionize(input)

    # TODO: Move this to its own function
    # TODO: Rename "PUT raw qplan"
    if "PUT raw qplan" in sections:
        raw_qplan = sections["PUT raw qplan"]
        # Create file
        if not os.path.exists("./raw"):
            os.makedirs("./raw")
        file = open("./raw/qplan.txt", "w")
        file.write(raw_qplan);
        file.close()

        # Add file and check in
        git_result = subprocess.call("git add ./raw/qplan.txt", shell=True)
        if git_result != 0:
            socket.send("ERROR: Couldn't add raw/qplan.txt")
            raise Exception("Problem adding raw/qplan.txt")
        git_result = subprocess.call("git commit -m 'Update raw qplan data'",
                shell=True)
        # TODO: Check if file hasn't changed
        #if git_result != 0:
        #    socket.send("ERROR: Couldn't commit raw/qplan.txt")
        #    raise Exception("Problem commiting raw/qplan.txt")

        # Announce that there's new raw data
        pub_socket.send("=====qplan raw");

        socket.send("OK")

    elif "GET raw qplan" in sections:
        version = sections["GET raw qplan"].split("\t")[0]
        if not version:
            version = "HEAD"

        p = subprocess.Popen("git show %s:raw/qplan.txt" % version,
                stdout=subprocess.PIPE, shell=True)
        input = io.StringIO(p.communicate()[0])

        p = subprocess.Popen("git rev-parse %s" % version,
                stdout=subprocess.PIPE, shell=True)
        rev = p.communicate()[0][0:5]

        header = ["=====raw qplan\n\t%s\n" % rev, "=====data\n"]
        data = []
        for l in input.readlines():
            data.append("\t%s" % l)
        message = "".join(header + data)
        socket.send_unicode(message)
    else:
        socket.send("ERROR: Unable to process any: %s" % sections.keys())
