#!/usr/bin/env python
import os
import sys
import json
import datetime
import urllib, urllib2
from itertools import combinations
from collections import defaultdict 
from utils import log, download, write, log_dir
import argparse

def assemble(pid, overwrite=False):
    if not overwrite:
        try: 
            info = json.load(open(os.getcwd() + "/data/api/signatures/" + pid + "/info.json", "r"))
            #print "Already got that one"
            return info
        except Exception, e:
            print "No info file found for petition %s. Computing" % pid
    
    signatures = []
    for filenm in [x for x in os.listdir(os.getcwd() + "/data/api/signatures/%s/" % pid) if len(x) > 11]:
        signatures += json.load(open(os.getcwd() + "/data/api/signatures/%s/%s" % (pid, filenm), 'r'))
        
    zips = [x for x in signatures if x["zip"] and x["zip"] != "" and x["name"] and x["name"] != ""]
    log("Found names and zip codes for %i percent of signatures on petition %s" % (100 * len(zips) / len(signatures), pid))
    uniques = ["%s_%s" % (x['name'], x['zip']) for x in zips]

    info = {
        "total": len(signatures),
        "zips": len(uniques),
        #"duplicates": list(set([(x, uniques.count(x)) for x in uniques if uniques.count(x) > 1])),
        "uniques": list(set(uniques))
    }

    write(json.dumps(info, indent=2), "api/signatures/%s/info.json" % pid)

    #print duplicates
    #uniques = set(uniques)
   
    return info

def get_roster(mx, offset, startat):
    roster = defaultdict(list)
    lineup = defaultdict(list)
    
    total = 0
    petitions = [x for x in os.listdir("data/api/petitions/") if x[-5:] == ".json"]
    if startat and startat + ".json" in petitions:
        offset = petitions.index(startat + ".json")
    petitions = petitions[offset:]
    
    if mx != -1:
        petitions = petitions[:mx]

    identities = [0,0]
    for petition in petitions:
        info = assemble(petition[:-5])
        lineup[petition] = info["uniques"]
        identities[0] += info["zips"]
        identities[1] += info["total"]
        
        for person in info["uniques"]:
            roster[person].append(petition)
        
    
    print identities, 100 * identities[0] / identities[1]
    '''
    multis = [x for x in roster.items() if len(x[1]) > 1]
    report = {
        'total': total,
        'uniques': len(roster.items()),
        'petitions': petitions,
        'multis': sorted(multis, key=lambda x:len(x[1]), reverse=True)
    }
    '''
    write(json.dumps(roster, indent=2), "api/reports/by_name.json")
    write(json.dumps(lineup, indent=2), "api/reports/by_petition.json")
    

def get_nodes():
    nodes = {}
    for petition in [x for x in os.listdir("data/api/petitions/") if x[-5:] == ".json"]:
        nodes[petition[:-5]] = json.load(open(os.getcwd() + "/data/api/petitions/" + petition, "r"))
    return nodes    

def get_edges():
    data = json.load(open(os.getcwd() + "/data/api/reports/by_petition.json", 'r'))
    keys = sorted(data.keys())
    edges ={}
    
    for (x,y) in combinations(keys, 2):
        if (x > y):
            x,y = y,x
        edges[x + "_" + y] = set(data[x]).intersection(set(data[y]))

    write(json.dumps(data, indent=2), "api/reports/edges.json")        




 
def networkify():
    data = json.load(open(os.getcwd() + "/data/api/reports/by_petition.json", 'r'))
    categories = json.load(open(os.getcwd() + "/data/api/reports/nodes.json", 'r'))
    keys = sorted(data.keys())
    
    nodes = [json.load(open(os.getcwd() + "/data/api/petitions/" + x, "r")) for x in keys]

    nodes = [x for x in nodes if x["signature count"] > 1000]

    keys = [x["id"] + ".json" for x in nodes]

    for node in nodes:
        node["type"] = categories[node["id"]]["category"]
    print len(nodes)
    edges = []
    
    edge_count = 0
    for (x,y) in combinations(keys, 2):
        #make sure keys are ordered -- important to avoid splitting links. Shouldn't be an issue since keys is sorted
        if (x > y):
            x,y = y,x
        #common signatures between the two petitions
        n = len(set(data[x]).intersection(set(data[y])))

        #two ways of measuring commonality
        #see math.stackexchange.com/questions/311524/how-do-i-best-weigh-the-commonality-between-sets-weighted-to-the-size-of-the-set
        i0 = float(n) / (len(data[x]) * len(data[y]))
        i1 = float(n) / (len(data[x]) + len(data[y]))

        if i1 > 0.075:
            edges.append({
                "n": n,
                "v0": i0,
                "value": i1,
                "source": keys.index(x),
                "target": keys.index(y)
            })
            edge_count += 1

    data = {
        "nodes": nodes,
        "links": edges
    }

    write(json.dumps(data, indent=2), "/Users/cewilson/Dropbox/Private/projects/whitehouse/data/network.json")        
    print edge_count

def get_nodes():
    data = json.load(open(os.getcwd() + "/data/api/reports/by_petition.json", 'r'))
    
    keys = sorted(data.keys())
    categories = {}
    for key in keys:
        p = json.load(open(os.getcwd() + "/data/api/petitions/" + key, "r"))
        categories[key[:-5]] = {
            "title": p["title"],
            "body": p["body"],
            "category": ""
        }

    write(json.dumps(categories, indent=2), "api/reports/nodes.json")

def analyze():
    data = json.load(open(os.getcwd() + "/data/api/reports/by_petition.json", 'r'))
    keys = sorted(data.keys())
    nodes = {}
    for key in keys:
        nodes[key] = json.load(open(os.getcwd() + "/data/api/petitions/" + key, "r"))

    data = json.load(open(os.getcwd() + "/data/api/reports/by_name.json", 'r'))

    dist = defaultdict(int)
    for x in data.items():
        dist[len(x[1])] += 1
    print dist
    print sum([len(x[1]) for x in data.items()])
    print sum([y for x,y in dist.items()])

    '''
    for x in data.items():
        if len(x[1]) > 150:
            print x[0]
            for pet in x[1]:
                print pet, nodes[pet]["title"]
    '''    
def main():
    parser = argparse.ArgumentParser(description="Retrieve petitions from the We The People API")
    parser.add_argument(metavar="TASK", dest="task", type=str, default="petitions",
                        help="which task to run: petitions, signatures")
    parser.add_argument("-m", "--max", metavar="INTEGER", dest="max", type=int, default=-1,
                        help="maximum number of petitions to retrieve")
    parser.add_argument("-s", "--start", metavar="INTEGER", dest="start", type=int, default=0,
                        help="starting page, 20 per page, default is 1")
    parser.add_argument("-b", "--by", metavar="INTEGER", dest="by", type=str, default="petition",
                        help="starting page, 20 per page, default is 1")
    parser.add_argument("-a", "--startat", dest="startat", type=str, default=None,
                        help="if of the first petition to crawl, in leiu of --start")
    parser.add_argument("-i", "--id", dest="pid", type=str, default=None,
                        help="the id of a single petition to crawl")

    args = parser.parse_args()
    
    #HumanError catch
    if args.max != -1 and args.max < 1:
        parser.error("How can I scrape less than one petition? You make no sense! --max must be one or greater.")
    if args.start < 0:
        parser.error("--start must be zero or greater.")
        
    #function calls    
    if args.task == "network":
        if args.pid:
            roster = assemble(args.pid, defaultdict(list))
        else:
            roster = get_roster(args.max, args.start, args.startat)
            print "done"
    elif args.task == "make":
        networkify()
        
    elif args.task == "analyze":
        analyze()
    elif args.task == "nodes":
        get_nodes()
    elif args.task == "edges":
        get_edges()

        
if __name__ == "__main__":
    main()
