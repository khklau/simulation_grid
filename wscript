import os
from waflib.ConfigSet import ConfigSet

def options(optCtx):
    optCtx.recurse('prop')
    optCtx.recurse('dep')
    optCtx.recurse('src')

def configure(confCtx):
    confCtx.recurse('prop')
    confCtx.recurse('dep')
    confCtx.recurse('src')

def build(buildCtx):
    buildCtx.recurse('prop')
    buildCtx.recurse('dep')
    buildCtx.recurse('src')

def install(installCtx):
    installCtx.recurse('prop')
    installCtx.recurse('src')
