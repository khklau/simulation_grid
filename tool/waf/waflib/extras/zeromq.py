#!/usr/bin/env python
# encoding: utf-8
# Kean H Lau, 2014

'''
This is an extra tool, not bundled with the default waf binary.
To add the Zeromq tool to the waf file:
    $ ./waf-light --tools=zeromq
or, if you have waf >= 1.6.2
    $ ./waf update --files=zeromq

When using this tool, the wscript will look like:

    def options(opt):
	opt.load('compiler_cxx zeromq')

    def configure(conf):
	conf.load('compiler_cxx zeromq')

    def build(bld):
	bld(source='main.cpp', target='app', use='ZEROMQ')

Options available are:
    --zeromq-incpath : the directory containing the header files
    --zeromq-libpath : the directory containing the library files
'''

import sys
import re
import os
from os.path import join
from waflib import Utils, Logs, Errors
from waflib.Configure import conf


def options(optCtx):
    optCtx.add_option('--zeromq-incpath', type='string',
	    default='', dest='zeromq_incpath',
	    help='''absolute path to the zeromq headers
	    e.g. /path/to/zeromq/include''')
    optCtx.add_option('--zeromq-libpath', type='string',
	    default='', dest='zeromq_libpath',
	    help='''absolute path to the zeromq libraries
	    e.g. /path/to/zeromq/lib''')

@conf
def check_zeromq(self):
    self.start_msg('Checking Zeromq headers')
    headerPath = join(self.env['INCLUDES_ZEROMQ'], 'zmq.hpp')
    if not os.access(headerPath, os.R_OK):
	self.fatal('%s is not readable' % headerPath)
    self.end_msg('ok')
    self.start_msg('Checking Zeromq libraries')
    for lib in self.env['LIB_ZEROMQ']: 
	libPath = join(self.env['LIBPATH_ZEROMQ'], "lib%s.so" % lib)
	if not os.access(libPath, os.R_OK):
	    self.fatal('%s is not readable' % libPath)
    self.end_msg('ok')

def configure(confCtx):
    platform = Utils.unversioned_sys_platform()
    if not confCtx.env['CXX']:
	confCtx.fatal('load a c++ compiler first, conf.load("compiler_cxx")')

    if confCtx.options.zeromq_incpath is not None and confCtx.options.zeromq_incpath != '':
	incpath = confCtx.options.zeromq_incpath
    elif platform == 'win32':
	incpath = 'C:\zeromq\include'
    else:
	incpath = '/usr/local/include'

    if confCtx.options.zeromq_libpath is not None and confCtx.options.zeromq_libpath != '':
	libpath = confCtx.options.zeromq_libpath 
    elif platform == 'win32':
	libpath = 'C:\zeromq\lib'
    else:
	libpath = '/usr/local/lib'

    confCtx.env['INCLUDES_ZEROMQ'] = incpath
    confCtx.env['LIBPATH_ZEROMQ'] = libpath
    confCtx.env['LIB_ZEROMQ'] = ['zmq']

    confCtx.check_zeromq()
