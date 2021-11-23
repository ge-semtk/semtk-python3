import semtk3
import sys
import argparse

#https://stackoverflow.com/questions/17909294/python-argparse-mutual-exclusive-group

def main():
    args = sys.argv[1:]
    print("args: " + ' '.join(args))