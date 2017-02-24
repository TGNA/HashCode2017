from threading import Thread
 
class Cache(object):
    
    def __init__(self, id, capacity):
        self.id = id
        self.capacity = capacity
        self.videos = {}
        # self.endPoints = []
    
    def usedSize(self):
        size = 0
        for v in self.videos.values():
            size += v.size
        return size
        
    def enoughSpace(self, size):
        return (self.capacity - self.usedSize()) >= size
    
    def getVideo(self, id):
        return self.videos[id]        
    
    def addVideo(self, video):
        self.videos[video.id]= video
        
    def removeVideo(self, id):
        self.videos.pop(id)

class Endpoint(object):
    
    def __init__(self,id, datacenterLatency):
        self.id = id
        self.requests = {}
        self.cacheLatencies = {}
        self.datacenterLatency = datacenterLatency

    def addRequest(self, videoId, nRequests):
        self.requests[videoId] = nRequests

    def getRequest(self, videoId):
        return self.requests[videoId]
    
class Video(object): 

    def __init__(self, id, size):
        self.id = id
        self.size = size

class Network(object):
            
    def __init__(self, file):
        self.videos = {}
        self.caches = {}
        self.endpoints = {}
        self.readFile(file)
        
    def calculate(self):
        for endpoint in self.endpoints.values():
            for videoId, request in endpoint.requests.iteritems():
                video = self.videos[videoId]
                maxPoints = 0.0
                bestCache = None
                
                for cacheId, latency in endpoint.cacheLatencies.iteritems():
                    points = float(request/latency)
                    
                    if (points > maxPoints):
                        maxPoints = points
                        bestCache = self.caches[cacheId]

                
                if(bestCache and bestCache.enoughSpace(video.size)):
                    bestCache.addVideo(video)

    def sendToFile(self, fileName):
        lines=[]
        ncaches=0
        for cache in self.caches.values():
            if (cache.usedSize() > 0):
                ids=""
                for idVideo in cache.videos.keys():
                    ids=ids+str(idVideo)+" "
                lines.append("\n"+str(cache.id)+" "+ids)
                ncaches+=1

        file = open("output/"+fileName, "w")
        file.write(str(ncaches))
        for line in lines:
            file.write(line)
        file.close()
        
    def readFile(self, file):
        file = open(file, "r")

        infoLine = file.readline()[:-1]
        
        nVideos, nEndpoints, nRequests, nCaches, cacheSize = map(int, infoLine.split(' '))
        
        for i in range(nCaches):
            self.caches[i] = Cache(i, cacheSize)

        # Next line, containing video sizes
        infoLine = file.readline()[:-1]
        
        for id, size in enumerate(infoLine.split(' ')):
            self.videos[id] = Video(id, int(size))
            
        #The next lines describe the endpoints
        for endpointId in range(nEndpoints):
            infoLine = file.readline()[:-1]
            datacenterLatency, conectedcaches = infoLine.split(' ')
            
            self.endpoints[endpointId] = Endpoint(endpointId, datacenterLatency)
            
            for cacheLine in range(int(conectedcaches)):
                infoLine = file.readline()[:-1]
                cacheid, cachelat = map(int, infoLine.split(' '))
                
                self.endpoints[endpointId].cacheLatencies[cacheid] = cachelat
                # self.caches[cacheid].endPoints.append(endpointId)
  
        #Requests
        for line in file:
            videoid, endpointid, nrequests = map(int, line.split(' '))
            self.endpoints[endpointid].requests[videoid] = int(nrequests)
        
        file.close()    

def main(file):
    print "Calculating "+file+"...\n"

    network = Network(file+".in")
    
    network.calculate()
    
    network.sendToFile(file+".out")

if __name__ == "__main__":
    # files = ["kittens", "me_at_the_zoo", "trending_today", "videos_worth_spreading"]
    # threads = []
    
    # for file in files:
    #     t = Thread(target = main, args = (file, ))
    #     t.start()
    #     threads.append(t)
    
    # for t in threads:
    #     t.join()
    
    # print "Finished!!"
    main("example")