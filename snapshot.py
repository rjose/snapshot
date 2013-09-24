# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import io
import os
import re
import subprocess

import zmq

from utils.sectionize import sectionize

class CommitError(Exception):
    pass

#TODO: Add functions to create stacked streams (these will eventually go into sectionize)


# TODO: Read these ports from the commandline
reply_port = 5000
publish_port = 5100

# Create 0mq context
context = zmq.Context()

# Socket for replying to snapshot requests from users (GETs and PUTs)
rep_socket = context.socket(zmq.REP)
rep_socket.bind("tcp://127.0.0.1:%d" % reply_port)

# Socket for publishing changes to resources
pub_socket = context.socket(zmq.PUB)
pub_socket.bind("tcp://127.0.0.1:%d" % publish_port)

# TODO: Set this from the commandline (default to current dir)
working_directory = "/home/rjose/dev/snapshot/temp"
os.chdir(working_directory)

def commit_file(file):
    # Add file
    git_result = subprocess.call("git add %s" % file, shell=True)
    if git_result != 0:
        raise CommitError("Problem adding %s" % file)

    # Commit file
    git_result = subprocess.call("git commit -m 'Update %s'" % file, shell=True)

    # TODO: Check if file hasn't changed
    #if git_result != 0:
    #    socket.send("ERROR: Couldn't commit raw/qplan.txt")
    #    raise Exception("Problem commiting raw/qplan.txt")


def put_resource(header, data, header_map):
    resource = header.split("PUT ")[1]
    filename = header_map[resource]
    dirname = os.path.dirname(filename)
    # TODO: Test this at least once
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # Write data to file
    file = open(filename, "w")
    file.write(data);
    file.close()

    # Check file in
    try:
        commit_file(filename)

        # Publish that file was checked in
        pub_socket.send_unicode("=====%s" % resource);
        print(resource)

        # Reply to client that PUT file
        rep_socket.send("OK")
    except CommitError:
        rep_socket.send_unicode("ERROR: Couldn't commit PUT %s" % resource)

# TODO: Move this to sectionize
def make_sections(section_lists):
    sections = []
    for sls in section_lists:
        header = "=====%s\n" % sls[0]
        data = sls[1]
        sections.append(header + data)
    result = "".join(sections)
    return result


def get_resource(header, data, header_map):
    resource = header.split("GET ")[1]
    filename = header_map[resource]
    version = data.split("\t")[0]
    if not version:
        version = "HEAD"
    print("Getting data for %s, %s" % (header, version))

    p = subprocess.Popen("git show %s:%s" % (version, filename),
            stdout=subprocess.PIPE, shell=True)
    contents = io.StringIO(p.communicate()[0])

    p = subprocess.Popen("git rev-parse %s" % version,
            stdout=subprocess.PIPE, shell=True)
    rev = p.communicate()[0][0:5]

    data = []
    for l in contents.readlines():
        data.append("\t%s" % l)
    new_contents = "".join(data)
    message = make_sections([["raw qplan", "\t%s\n" % rev], ["data", new_contents]])
    rep_socket.send_unicode(message)


def event_loop(header_map):
    while True:
        message = io.StringIO(rep_socket.recv())
        # TODO: Handle errors in sectionizing
        sections = sectionize(message)

        for header in sections.keys():
            if re.match("PUT", header):
                put_resource(header, sections[header], header_map)
            elif re.match("GET", header):
                get_resource(header, sections[header], header_map)
            else:
                print(header)
                rep_socket.send_unicode("TODO: Handle %s" % header)


#===============================================================================
# Code that should be in a custom script
#
header_file_map = {
        "qplan raw": "qplan_raw.txt",
        "qplan cond": "qplan_cond.txt",
        "qplan app": "qplan_app.txt"
}
event_loop(header_file_map)

