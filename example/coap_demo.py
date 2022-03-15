import getopt
import socket
import sys
import time

from coapthon.client.helperclient import HelperClient
from coapthon.utils import parse_uri

client = None


def usage():  # pragma: no cover
    print("Command:\tcoapclient.py -o -p [-P]")
    print("Options:")
    print("\t-o, --operation=\tGET|PUT|POST|DELETE|DISCOVER|OBSERVE")
    print("\t-p, --path=\t\t\tPath of the request")
    print("\t-P, --payload=\t\tPayload of the request")
    print("\t-f, --payload-file=\t\tFile with payload of the request")


def main():  # pragma: no cover
    global client
    op = None
    path = None
    payload = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:p:P:f:", ["help", "operation=", "path=", "payload=",
                                                               "payload_file="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(str(err))  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    for o, a in opts:
        if o in ("-o", "--operation"):
            op = a
        elif o in ("-p", "--path"):
            path = a
        elif o in ("-P", "--payload"):
            payload = a
        elif o in ("-f", "--payload-file"):
            with open(a, 'r') as f:
                payload = f.read()
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            usage()
            sys.exit(2)

    if op is None:
        print("Operation must be specified")
        usage()
        sys.exit(2)

    if path is None:
        print("Path must be specified")
        usage()
        sys.exit(2)

    if not path.startswith("coap://"):
        print("Path must be conform to coap://host[:port]/path")
        usage()
        sys.exit(2)

    host, port, path = parse_uri(path)
    try:
        tmp = socket.gethostbyname(host)
        host = tmp
    except socket.gaierror:
        pass
    client = HelperClient(server=(host, port))
    if op == "PUT":
        if payload is None:
            print("Payload cannot be empty for a PUT request")
            usage()
            sys.exit(2)
        while True:
            try:
                response = client.put(path, payload)
                print(response.pretty_print())
                time.sleep(3)
            except Exception as e:
                print(e.__str__())
                client.stop()
                sys.exit(2)


if __name__ == '__main__':
    main()
