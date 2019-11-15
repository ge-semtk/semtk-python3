from . import edcclient
#
# A client to HiveService
#
class HiveClient(edcclient.EdcClient):
    
    def __init__(self, serverURL, hiveserver_host, hiveserver_port, hiveserver_database, status_client=None, results_client=None):
        ''' serverURL string - e.g. http://machine:12055
        '''
        super(HiveClient, self).__init__(serverURL, "hiveService", status_client, results_client)
        self.hiveserverhost = hiveserver_host
        self.hiveserverport = hiveserver_port
        self.hiveserverdatabase = hiveserver_database
 
 
    #
    # Execute query.
    #
    def exec_query_hive(self, query):    
        payload = {
            "host": self.hiveserverhost,
            "port": self.hiveserverport,
            "database": self.hiveserverdatabase,
            "query": query
        }
        table = self.post_to_table("queryHive", payload)
        return table
    