import os
from waflib.ConfigSet import ConfigSet

def options(optCtx):
    optCtx.recurse('env')
    optCtx.recurse('dep')
    optCtx.recurse('main')

def configure(confCtx):
    confCtx.recurse('env')
    confCtx.recurse('dep')
    confCtx.recurse('main')

def build(buildCtx):
    buildCtx.recurse('dep')
    buildCtx.recurse('main')

def install(installCtx):
    installCtx.recurse('main')
