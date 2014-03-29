#!/usr/bin/env python
# encoding: utf-8
# Kean H Lau, 2014

'''
This is an extra tool, not bundled with the default waf binary.
To add the Gtest tool to the waf file:
    $ ./waf-light --tools=gtest
or, if you have waf >= 1.6.2
    $ ./waf update --files=gtest

When using this tool, the wscript will look like:

    def options(opt):
	opt.load('compiler_cxx gtest')

    def configure(conf):
	conf.load('compiler_cxx gtest')

    def build(bld):
	bld(source='main.cpp', target='app', use='GTEST')

Options available are:
    --gtest-incpath : the directory containing the header files
    --gtest-libpath : the directory containing the library files
'''

import sys
import re
import os
from os.path import join
from waflib import Utils, Logs, Errors
from waflib.Configure import conf


def options(optCtx):
    optCtx.add_option('--gtest-incpath', type='string',
	    default='', dest='gtest_incpath',
	    help='''absolute path to the gtest headers
	    e.g. /path/to/gtest/include''')
    optCtx.add_option('--gtest-libpath', type='string',
	    default='', dest='gtest_libpath',
	    help='''absolute path to the gtest libraries
	    e.g. /path/to/gtest/lib''')

@conf
def check_gtest(self):
    self.start_msg('Checking Gtest headers')
    headerPath = join(self.env['INCLUDES_GTEST'], 'gtest', 'gtest.h')
    if not os.access(headerPath, os.R_OK):
	self.fatal('%s is not readable' % headerPath)
    self.end_msg('ok')
    self.start_msg('Checking Gtest libraries')
    for lib in self.env['STLIB_GTEST']: 
	libPath = join(self.env['STLIBPATH_GTEST'], "lib%s.a" % lib)
	if not os.access(libPath, os.R_OK):
	    self.fatal('%s is not readable' % libPath)
    self.end_msg('ok')

def configure(confCtx):
    platform = Utils.unversioned_sys_platform()
    if not confCtx.env['CXX']:
	confCtx.fatal('load a c++ compiler first, conf.load("compiler_cxx")')

    if confCtx.options.gtest_incpath is not None and confCtx.options.gtest_incpath != '':
	incpath = confCtx.options.gtest_incpath
    elif platform == 'win32':
	incpath = 'C:\gtest\include'
    else:
	incpath = '/usr/local/include'

    if confCtx.options.gtest_libpath is not None and confCtx.options.gtest_libpath != '':
	libpath = confCtx.options.gtest_libpath 
    elif platform == 'win32':
	libpath = 'C:\gtest\lib'
    else:
	libpath = '/usr/local/lib'

    confCtx.env['INCLUDES_GTEST'] = incpath
    confCtx.env['STLIBPATH_GTEST'] = libpath
    confCtx.env['STLIB_GTEST'] = ['gtest', 'gtest_main']

    confCtx.check_gtest()
