#!/usr/bin/env python
import subprocess
import argparse
import re

class regress:
    def __init__(self):
        self.platforms = {
            'default'   : '',
            'cloudant1' : ' -P accounts/cloudant1-flux7-1.props',
            'cloudant2' : ' -P accounts/cloudant2-flux7-2.props',
        }
        self.workloads = {
            'equal'     : ' -P workloads/workloada',
            'readmostly': ' -P workloads/workloadb',
            'readinsert': ' -P workloads/workloadd',
            'rmw'       : ' -P workloads/workloadf',
        }
        self.configs   = {
            'default'   : "",
            '15m'       : ' -P configs/15m.dat',
            'large'     : ' -P configs/large.dat',
        }

    def run_tests(self,platforms,workloads,configs):
        for platform in platforms:
            for workload in workloads:
                for config in configs:
                    base_cmd = "couchdb "+self.platforms[platform]+\
                               self.workloads[workload]+self.configs[config]+\
                               ' -P configs/1m.ops'
                    load_cmd = "bin/ycsb load "+base_cmd
                    run_cmd = "bin/ycsb run "+base_cmd+' -threads 128'
                    try:
                        load_log = subprocess.check_output(str.split(load_cmd))
                        run_log = subprocess.check_output(str.split(run_cmd))
                    except subprocess.CalledProcessError, e:
                        print "Non-zero error code, output:\n",e.output
                    print "Command:\n",run_cmd,"\n\nOutput:\n",run_log
                    # results[platform][workload][config]['run_log'] = run_log
                    # throughput = re.search('\[OVERALL\], Throughput\(ops\/sec\), ([0-9]|\.)+',run_log)
                    # results[platform][workload][config]['throughput']

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
