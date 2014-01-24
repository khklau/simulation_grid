#!/usr/bin/env python
# encoding: utf-8

'''
This is an extra tool, not bundled with the default waf binary.
To add the Gmock tool to the waf file:
    $ ./waf-light --tools=gmock
or, if you have waf >= 1.6.2
    $ ./waf update --files=gmock

When using this tool, the wscript will look like:

    def options(opt):
	opt.load('compiler_cxx gmock')

    def configure(conf):
	conf.load('compiler_cxx gmock')

    def build(bld):
	bld(source='main.cpp', target='app', use='GMOCK')

Options available are:
    --gmock-incpath : the directory containing the header files
    --gmock-libpath : the directory containing the library files
'''

import sys
import re
import os
from os.path import join
from waflib import Utils, Logs, Errors
from waflib.Configure import conf


def options(optCtx):
    optCtx.add_option('--gmock-incpath', type='string',
	    default='', dest='gmock_incpath',
	    help='''absolute path to the gmock headers
	    e.g. /path/to/gmock/include''')
    optCtx.add_option('--gmock-libpath', type='string',
	    default='', dest='gmock_libpath',
	    help='''absolute path to the gmock libraries
	    e.g. /path/to/gmock/lib''')

@conf
def check_gmock(self):
    self.start_msg('Checking Gmock headers')
    headerPath = join(self.env['INCLUDES_GMOCK'], 'gmock', 'gmock.h')
    if not os.access(headerPath, os.R_OK):
	self.fatal('%s is not readable' % headerPath)
    self.end_msg('ok')
    self.start_msg('Checking Gmock libraries')
    for lib in self.env['STLIB_GMOCK']: 
	libPath = join(self.env['STLIBPATH_GMOCK'], "lib%s.a" % lib)
	if not os.access(libPath, os.R_OK):
	    self.fatal('%s is not readable' % libPath)
    self.end_msg('ok')

def configure(confCtx):
    platform = Utils.unversioned_sys_platform()
    if not confCtx.env['CXX']:
	confCtx.fatal('load a c++ compiler first, conf.load("compiler_cxx")')

    if confCtx.options.gmock_incpath is not None and confCtx.options.gmock_incpath != '':
	incpath = confCtx.options.gmock_incpath
    elif platform == 'win32':
	incpath = 'C:\gmock\include'
    else:
	incpath = '/usr/local/include'

    if confCtx.options.gmock_libpath is not None and confCtx.options.gmock_libpath != '':
	libpath = confCtx.options.gmock_libpath 
    elif platform == 'win32':
	libpath = 'C:\gmock\lib'
    else:
	libpath = '/usr/local/lib'

    confCtx.env['INCLUDES_GMOCK'] = incpath
    confCtx.env['STLIBPATH_GMOCK'] = libpath
    confCtx.env['STLIB_GMOCK'] = ['gmock', 'gmock_main']

    confCtx.check_gmock()
