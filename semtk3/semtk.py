import semtk3
import sys
import argparse

#https://stackoverflow.com/questions/17909294/python-argparse-mutual-exclusive-group

# add sei args to a sub-parser
def add_sei_args(parser):
    parser.add_argument("triplestore_type")
    parser.add_argument("triplestore_url")
    parser.add_argument("graph")
    
# put the sei_args into a conn_str as data[0] and model[0]
def get_conn_str(args):
    conn_str = semtk3.build_connection_str("temp", args.triplestore_type, args.triplestore_url, [args.graph], args.graph, [])
    return conn_str
    
def main(command_line=None):

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')

    # import
    subparser_import = subparsers.add_parser("import", help="upload owl or ttl to a graph")
    add_sei_args(subparser_import)
    subparser_import.add_argument("owl_or_ttl_file")
    
    # clear
    subparser_clear = subparsers.add_parser("clear", help="clear a graph")
    add_sei_args(subparser_clear)
    
    # download
    subparser_download = subparsers.add_parser("download", help="download entire graph to local file")
    subparser_download.add_argument("format", choices=["owl"])
    add_sei_args(subparser_download)
    
    # store
    subparser_store = subparsers.add_parser("store", help="add a folder of store items to the store")
    subparser_store.add_argument("semtk_host")
    subparser_store.add_argument("folder")
    
    # retrieve
    subparser_retrieve = subparsers.add_parser("retrieve", help="retrieve matching items from the store to local folder")
    subparser_retrieve.add_argument("semtk_host")
    subparser_retrieve.add_argument("regex")
    subparser_retrieve.add_argument("folder")

    args = parser.parse_args(command_line)

    if args.command == "import":
        if args.owl_or_ttl_file.lower().endswith(".owl"):
            semtk3.upload_owl(args.owl_or_ttl_file, get_conn_str(args), "", "", "model", 0)
        elif args.owl_or_ttl_file.lower().endswith(".ttl"):
            semtk3.upload_turtle(args.owl_or_ttl_file, get_conn_str(args), "", "", "model", 0)
        else:
            raise Exception("File is not .owl or .ttl: " + args.owl_or_ttl_file)
        
    elif args.command == "clear":
        semtk3.clear_graph(get_conn_str(args), "model", 0)
        
    elif args.command == "download":
        if args.format == "owl":
            semtk3.download_owl
        semtk3.download_owl(None, get_conn_str(args), "", "", "model", 0)
        
    elif args.command == "store":
        semtk3.set_host(args.semtk_host)
        semtk3.store_folder(args.folder)
    
    elif args.command == "retrieve":
        semtk3.set_host(args.semtk_host)
        semtk3.retrieve_items_from_store(args.regex, args.folder)
        
if __name__ == '__main__':
    main() 