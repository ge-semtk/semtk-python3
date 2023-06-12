import semtk3
import sys
import argparse
import json
from semtk3 import stitchingstep

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

def file_to_string(filename):
    with open(filename, "r") as file:
        return file.read()
    
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
    
    # stitch
    subparser_stitch = subparsers.add_parser("stitch", help="run multiple nodegroups, stitching results")
    subparser_stitch.add_argument("semtk_host")
    subparser_stitch.add_argument("stitch_file", help='[{"nodegroupId": "name1"}, {"nodegroupId": "name2", "keyColumns": ["id"]')
    subparser_stitch.add_argument("-c", "--conn", required=False, help='connection file')
    
    # fdc_cache
    subparser_fdc_cache = subparsers.add_parser("fdc_cache", help="run an fdc cache spec")
    subparser_fdc_cache.add_argument("semtk_host")
    subparser_fdc_cache.add_argument("spec_id")
    subparser_fdc_cache.add_argument("conn_file", help='connection file')
    
    # query
    subparser_query = subparsers.add_parser("query", help="run query by nodegroup id to table csv")
    subparser_query.add_argument("semtk_host")
    subparser_query.add_argument("nodegroup_id")
    subparser_query.add_argument("-c", "--conn", required=False, help='connection file')

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
        
    elif args.command == "stitch":
        semtk3.set_host(args.semtk_host)
        
        steps_json_array = json.loads(file_to_string(args.stitch_file))
        step_array = [stitchingstep.StitchingStep(x["nodegroupId"], x["keyColumns"] if "keyColumns" in x else None) for x in steps_json_array]
        
        conn_json_str = file_to_string(args.conn) if args.conn is not None else "NODEGROUP_DEFAULT"
        
        print(semtk3.dispatch_stitched_nodegroups(step_array, conn_json_str)
                .get_csv_string().encode('cp850', errors='replace').decode('cp850'))                         #  handle the wonkiest of non-ascii non-utf8 chars
        
    elif args.command == "query":
        semtk3.set_host(args.semtk_host)
        
        if args.conn is not None: 
            semtk3.set_connection_override(file_to_string(args.conn))
        
        print(semtk3.query_by_id(args.nodegroup_id, 0, 0, None, None, None, semtk3.QUERY_TYPE_SELECT_DISTINCT, semtk3.RESULT_TYPE_TABLE)
                .get_csv_string().encode('cp850', errors='replace').decode('cp850'))                         #  handle the wonkiest of non-ascii non-utf8 chars
        
    elif args.command == "fdc_cache":
        semtk3.set_host(args.semtk_host)
        
        print(semtk3.run_fdc_cache_spec(args.spec_id, file_to_string(args.conn_file)))
        
if __name__ == '__main__':
    main() 