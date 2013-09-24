# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import io
import os
import re
import subprocess
import zmq

from utils.sectionize import sectionize

class CommitError(Exception):
    pass

# TODO: Move this to sectionize
def make_sections(section_lists):
    sections = []
    for sls in section_lists:
        header = "=====%s\n" % sls[0]
        data = sls[1]
        sections.append(header + data)
    result = "".join(sections)
    return result


class SnapshotService:
    """Manages snapshotting data files"""

    #===========================================================================
    # Public API
    #

    def __init__(self, header_file_map, repo_dir, reply_port, publish_port):
        self.header_file_map = header_file_map
        context = zmq.Context()

        # Socket for replying to snapshot requests from users (GETs and PUTs)
        self.rep_socket = context.socket(zmq.REP)
        self.rep_socket.bind("tcp://127.0.0.1:%d" % reply_port)
        
        # Socket for publishing changes to resources
        self.pub_socket = context.socket(zmq.PUB)
        self.pub_socket.bind("tcp://127.0.0.1:%d" % publish_port)

        # Set working directory to the repo
        os.chdir(repo_dir)


    def run(self):
        try:
            while True:
                message = io.StringIO(self.rep_socket.recv())
                # TODO: Handle errors in sectionizing
                sections = sectionize(message)
        
                for header in sections.keys():
                    if re.match("PUT", header):
                        self.put_resource(header, sections[header], self.header_file_map)
                    elif re.match("GET", header):
                        self.get_resource(header, sections[header], self.header_file_map)
                    else:
                        print(header)
                        self.rep_socket.send_unicode("TODO: Handle %s" % header)
        except Exception as e:
            print("EXCEPTION: %s" % str(e))


    #===========================================================================
    # Internal functions
    #

    def commit_file(self, file):
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


    def put_resource(self, header, data, header_map):
        resource = header.split("PUT ")[1]
        filename = header_map[resource]
        dirname = os.path.dirname(filename)
        # TODO: Test this at least once
        if not (os.path.exists(dirname) or dirname == ""):
            os.makedirs(dirname)
    
        # Write data to file
        file = open(filename, "w")
        file.write(data);
        file.close()
    
        # Check file in
        try:
            self.commit_file(filename)
    
            # Publish that file was checked in
            self.pub_socket.send_unicode("=====%s" % resource);
    
            # Reply to client that PUT file
            self.rep_socket.send("OK")
        except CommitError:
            self.rep_socket.send_unicode("ERROR: Couldn't commit PUT %s" % resource)
    
    
    def get_resource(self, header, data, header_map):
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
        self.rep_socket.send_unicode(message)




#===============================================================================
# Code that should be in a custom script
#
# TODO: Read these ports from the commandline
reply_port = 5000
publish_port = 5100
working_directory = "/home/rjose/dev/snapshot/temp"

header_file_map = {
        "qplan raw": "qplan_raw.txt",
        "qplan cond": "qplan_cond.txt",
        "qplan app": "qplan_app.txt"
}

service = SnapshotService(header_file_map, working_directory,
                                                    reply_port, publish_port)
service.run()
