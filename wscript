import os
from waflib.ConfigSet import ConfigSet

def options(optCtx):
    optCtx.recurse('prop')
    optCtx.recurse('dep')
    optCtx.recurse('main')

def configure(confCtx):
    confCtx.recurse('prop')
    confCtx.recurse('dep')
    confCtx.recurse('main')

def build(buildCtx):
    buildCtx.recurse('prop')
    buildCtx.recurse('dep')
    buildCtx.recurse('main')

def install(installCtx):
    installCtx.recurse('prop')
    installCtx.recurse('main')
