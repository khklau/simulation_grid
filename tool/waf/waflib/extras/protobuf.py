#!/usr/bin/env python
# encoding: utf-8
# Philipp Bender, 2012
# Matt Clarkson, 2012

'''
This is an extra tool, not bundled with the default waf binary.
To add the Protocol Buffer tool to the waf file:
    $ ./waf-light --tools=protobuf
or, if you have waf >= 1.6.2
    $ ./waf update --files=protobuf

When using this tool, the wscript will look like:

    def options(opt):
	opt.load('compiler_cxx cxx protobuf')

    def configure(conf):
	conf.load('compiler_cxx cxx protobuf')

    def build(bld):
	bld(source='msg.proto', target='app', features = 'cxx', use='PROTOBUF')

Options available are:
    --protobuf-binpath : the directory containing the compiler
    --protobuf-incpath : the directory containing the header files
    --protobuf-libpath : the directory containing the library files
'''

import sys
import re
import os
from os.path import join
import waflib.Build
from waflib import Utils
from waflib.Configure import conf
from waflib.Task import Task
from waflib.TaskGen import extension 

class protoc(Task):
    run_str = '${PROTOC} ${PROTOC_FLAGS} ${PROTOC_ST:INCPATHS} ${SRC[0].abspath()}'
    color   = 'BLUE'
    ext_out = ['.h', 'pb.cc']

@extension('.proto')
def process_protoc(self, node):
    cpp_node = node.change_ext('.pb.cc')
    hpp_node = node.change_ext('.pb.h')
    self.create_task('protoc', node, [cpp_node, hpp_node])
    self.source.append(cpp_node)
    self.target.append(cpp_node)
    self.target.append(hpp_node)

    if 'cxx' in self.features and not self.env['PROTOC_FLAGS']:
	self.env['PROTOC_FLAGS'] = '--cpp_out=%s' % node.parent.get_bld().abspath()

    use = getattr(self, 'use', '')
    if not 'PROTOBUF' in use:
	self.use = self.to_list(use) + ['PROTOBUF']

def proto_cxx_compile(task):
    output_cc = task.inputs[0].change_ext('.pb.cc')
    output_h = task.inputs[0].change_ext('.pb.h')
    task.env['PROTOC_FLAGS'] = '--cpp_out=%s' % task.inputs[0].parent.get_bld().abspath()
    cmd = '%s %s -I%s %s' % (task.env['PROTOC'], task.env['PROTOC_FLAGS'])
    return task.exec_command(cmd)

def define_task_gen(context, **keywords):
    context(name=keywords['name'],
	    rule='${PROTOC} --cpp_out=%s ${PROTOC_FLAGS} -I%s ${SRC[0].abspath()}' % (
                        context.path.get_bld().abspath(),
                        context.path.get_src().abspath()),
	    source=keywords['source'],
	    target=keywords['target'],
	    includes=keywords['includes'])

def options(optCtx):
    optCtx.add_option('--protobuf-binpath', type='string',
	    default='', dest='protobuf_binpath',
	    help='''absolute path to the protobuf compiler
	    e.g. /path/to/protobuf/bin''')
    optCtx.add_option('--protobuf-incpath', type='string',
	    default='', dest='protobuf_incpath',
	    help='''absolute path to the protobuf headers
	    e.g. /path/to/protobuf/include''')
    optCtx.add_option('--protobuf-libpath', type='string',
	    default='', dest='protobuf_libpath',
	    help='''absolute path to the protobuf libraries
	    e.g. /path/to/protobuf/lib''')

@conf
def check_protobuf(self):
    self.start_msg('Checking Protocol Buffer compiler')
    if not os.access(self.env['PROTOC'], os.X_OK):
	self.fatal('%s is not executable' % self.env['PROTOC'])
    self.end_msg('ok')
    self.start_msg('Checking Protocol Buffer headers')
    headerPath = join(self.env['INCLUDES_PROTOBUF'], 'google', 'protobuf', 'message.h')
    if not os.access(headerPath, os.R_OK):
	self.fatal('%s is not readable' % headerPath)
    self.end_msg('ok')
    self.start_msg('Checking Protocol Buffer libraries')
    for lib in self.env['LIB_PROTOBUF']: 
	libPath = join(self.env['LIBPATH_PROTOBUF'], "lib%s.a" % lib)
	if not os.access(libPath, os.R_OK):
	    self.fatal('%s is not readable' % libPath)
    self.end_msg('ok')

def configure(confCtx):
    platform = Utils.unversioned_sys_platform()
    if not confCtx.env['CXX']:
	confCtx.fatal('load a c++ compiler first, conf.load("compiler_cxx")')

    if confCtx.options.protobuf_binpath is not None and confCtx.options.protobuf_binpath != '':
	binpath = confCtx.options.protobuf_binpath
    elif platform == 'win32':
	binpath = 'C:\protobuf\bin'
    else:
	binpath = '/usr/local/bin'

    if platform == 'win32':
	protocpath = join(binpath, 'protoc.exe')
    else:
	protocpath = join(binpath, 'protoc')

    if confCtx.options.protobuf_incpath is not None and confCtx.options.protobuf_incpath != '':
	incpath = confCtx.options.protobuf_incpath
    elif platform == 'win32':
	incpath = 'C:\protobuf\include'
    else:
	incpath = '/usr/local/include'

    if confCtx.options.protobuf_libpath is not None and confCtx.options.protobuf_libpath != '':
	libpath = confCtx.options.protobuf_libpath 
    elif platform == 'win32':
	libpath = 'C:\protobuf\lib'
    else:
	libpath = '/usr/local/lib'

    confCtx.env['PROTOC'] = protocpath
    confCtx.env['INCLUDES_PROTOBUF'] = incpath
    confCtx.env['LIBPATH_PROTOBUF'] = libpath
    confCtx.env['LIB_PROTOBUF'] = ['protobuf']
    confCtx.env['PROTOC_ST'] = '-I%s'

    confCtx.check_protobuf()

