#!/usr/bin/env python
from subprocess import call
import argparse

class regress:
    def __init__(self):
        self.platforms = {
            'default'   : '',
            'cloudant1' : ' -P accounts/cloudant1-flux7-1.props',
            'cloudant2' : ' -P accounts/cloudant2-flux7-2.props',
        }
        self.workloads = {
            'equal'     : ' -P workload/workloada',
            'readmostly': ' -P workload/workloadb',
            'readinsert': ' -P workload/workloadd',
            'rmw'       : ' -P workload/workloadf',
        }
        self.configs   = {
            'default'   : ' -P configs/large.dat',
        }
    def run_tests(self,platforms,workloads,configs):
        for platform in platforms:
           for workload in workloads:
               for config in configs:
                   print "bin/ycsb load "+self.platforms[platform]+self.workloads[workload]+\
                       self.configs[config]
    def launch(self,args):
        p = str.split( args.platforms,',')
        w = str.split( args.workloads,',')
        c = str.split( args.configs,',')
        error = False
        for platform in p:
            if not platform in self.platforms:
                print "Unknown platform: ",platform
                error = True
        for workload in w:
            if not workload in self.workloads:
                print "Unknown workload: ",workload
                error = True
        for config in c:
            if not config in self.configs:
                print "Unknown config: ",config
                error = True
        if error == False:
            self.run_tests(p,w,c)

def main(args):
    r = regress()
    r.launch(args)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check all ec2 instances and run a command')
    parser.add_argument('-p','--platforms',dest='platforms',default='default')
    parser.add_argument('-w','--workloads',dest='workloads',default='equal')
    parser.add_argument('-c','--configs',dest='configs',default='default')
    args = parser.parse_args()
    main(args)
