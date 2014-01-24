#!/usr/bin/env python
# encoding: utf-8

'''
This is an extra tool, not bundled with the default waf binary.
To add the layout tool to the waf file:
    $ ./waf-light --tools=layout
or, if you have waf >= 1.6.2
    $ ./waf update --files=layout

When using this tool, the wscript will look like:

    from waflib.extras.layout import Solution, Product, Component

    PRODUCT_NAME = 'foo'
    COMPONENT_NAME = 'bar'

    def configure(conf):
	conf.env.solution = Solution.fromContext(conf)
	conf.env.product = Product.fromContext(conf, PRODUCT_NAME, conf.env.solution)
	conf.env.solution.addProduct(PRODUCT_NAME, conf.env.product)
	conf.env.component = Component(conf, COMPONENT_NAME, conf.env.product)
	conf.env.product.addComponent(COMPONENT_NAME, conf.env.component)

    def build(bld):
	bld.env.product = bld.env.solution.getProduct(PRODUCT_NAME)
	bld.env.component = bld.env.product.getComponent(COMPONENT_NAME)
	bld.shlib(source=['blah.c'], target='blah', install_path=bld.env.component.install_tree.lib)
	bld(rule='cp ${SRC} ${TGT}', source='blah.h', target=bld.env.component.build_tree.include)
'''

import sys
from waflib import Node
from waflib.Context import Context

class InputTree:

    __slots__ = ('root', 'src', 'doc', 'include', 'test')

    def __init__(self, root, src, doc, include, test):
	self.root = root
	self.src = src
	self.doc = doc
	self.include = include
	self.test = test

    @classmethod
    def fromNode(cls, root, src, doc, include, test):
	return cls(root.abspath(), src.abspath(), doc.abspath(),
		include.abspath(), test.abspath())

    def __repr__(self):
	return "__import__('waflib').extras.layout.%s('%s', '%s', '%s', '%s', '%s')" % (
		self.__class__.__name__, self.root, self.src, 
		self.doc, self.include, self.test)

    def rootNode(self, context):
	return context.root.make_node(self.root)

    def srcNode(self, context):
	return context.root.make_node(self.src)

    def docNode(self, context):
	return context.root.make_node(self.doc)

    def includeNode(self, context):
	return context.root.make_node(self.include)

    def testNode(self, context):
	return context.root.make_node(self.test)


class OutputTree:

    __slots__ = ('root', 'bin', 'lib', 'doc', 'include', 'test')

    def __init__(self, root, bin, lib, doc, include, test):
	self.root = root
	self.bin = bin
	self.lib = lib
	self.doc = doc
	self.include = include
	self.test = test

    @classmethod
    def fromNode(cls, root, bin, lib, doc, include, test):
	return cls(root.abspath(), bin.abspath(), lib.abspath(), doc.abspath(),
		include.abspath(), test.abspath())

    def __repr__(self):
	return "__import__('waflib').extras.layout.%s('%s', '%s', '%s', '%s', '%s', '%s')" % (
		self.__class__.__name__, self.root, self.bin, 
		self.lib, self.doc, self.include, self.test)

    def rootNode(self, context):
	return context.root.make_node(self.root)

    def binNode(self, context):
	return context.root.make_node(self.bin)

    def libNode(self, context):
	return context.root.make_node(self.lib)

    def docNode(self, context):
	return context.root.make_node(self.doc)

    def includeNode(self, context):
	return context.root.make_node(self.include)

    def testNode(self, context):
	return context.root.make_node(self.test)


class Component:

    __slots__ = ('name', 'build_tree', 'install_tree', 'include_path_list', 'lib_path_list', 'rpath_list', 'input_tree')

    def __init__(self, name, build_tree, install_tree, include_path_list, lib_path_list, rpath_list, input_tree):
	self.name = name
	self.build_tree = build_tree
	self.install_tree = install_tree
	self.include_path_list = include_path_list
	self.lib_path_list = lib_path_list
	self.rpath_list = rpath_list
	self.input_tree = input_tree

    @classmethod
    def fromContext(cls, context, name, product):
	input = InputTree.fromNode(context.path,
		context.path.make_node('src'), context.path.make_node('doc'),
		context.path.make_node('include'), context.path.make_node('test'))
	return cls(name, OutputTree.fromNode(
		product.build_tree.rootNode(context), product.build_tree.binNode(context),
		product.build_tree.libNode(context), product.build_tree.docNode(context),
		product.build_tree.includeNode(context).make_node(name),
		product.build_tree.testNode(context).make_node(name)),
		OutputTree.fromNode(
		product.install_tree.rootNode(context), product.install_tree.binNode(context),
		product.install_tree.libNode(context), product.install_tree.docNode(context),
		product.install_tree.includeNode(context).make_node(name),
		product.install_tree.testNode(context).make_node(name)),
		[input.include] + product.include_path_list, list(product.lib_path_list),
		["\$ORIGIN/../lib"] + product.lib_path_list + [product.install_tree.lib], input)

    def __repr__(self):
	return "__import__('waflib').extras.layout.%s('%s', %s, %s, %s, %s, %s, %s)" % (
		self.__class__.__name__, self.name,
		self.build_tree.__repr__(), self.install_tree.__repr__(),
		self.include_path_list.__repr__(), self.lib_path_list.__repr__(),
		self.rpath_list.__repr__(), self.input_tree.__repr__())


class Product:

    __slots__ = ('name', 'build_tree', 'install_tree', 'include_path_list', 'lib_path_list')

    def __init__(self, name, build_tree, install_tree, include_path_list, lib_path_list, component_set):
	self.name = name
	self.build_tree = build_tree
	self.install_tree = install_tree
	self.include_path_list = include_path_list
	self.lib_path_list = lib_path_list
	self.__component_set = component_set

    @classmethod
    def fromContext(cls, context, name, solution):
	return cls(name, OutputTree.fromNode(
		solution.build_tree.rootNode(context), solution.build_tree.binNode(context),
		solution.build_tree.libNode(context), solution.build_tree.docNode(context),
		solution.build_tree.includeNode(context).make_node(name),
		solution.build_tree.testNode(context).make_node(name)),
		OutputTree.fromNode(
		solution.install_tree.rootNode(context), solution.install_tree.binNode(context),
		solution.install_tree.libNode(context), solution.install_tree.docNode(context),
		solution.install_tree.includeNode(context).make_node(name),
		solution.install_tree.testNode(context).make_node(name)),
		list(solution.include_path_list), list(solution.lib_path_list), dict())

    def __repr__(self):
	return "__import__('waflib').extras.layout.%s('%s', %s, %s, %s, %s, %s)" % (
		self.__class__.__name__, self.name, self.build_tree.__repr__(),
		self.install_tree.__repr__(), self.include_path_list.__repr__(),
		self.lib_path_list.__repr__(), self.__component_set.__repr__())

    def addComponent(self, component):
	self.__component_set[component.name] = component

    def getComponent(self, name):
	return self.__component_set[name]


class Solution:

    __slots__ = ('build_tree', 'install_tree', 'include_path_list', 'lib_path_list')

    def __init__(self, build_tree, install_tree, include_path_list, lib_path_list, product_set):
	self.build_tree = build_tree
	self.install_tree = install_tree
	self.include_path_list = include_path_list
	self.lib_path_list = lib_path_list
	self.__product_set = product_set

    @classmethod
    def fromContext(cls, context):
	if context.options.out != "":
	    buildNode = context.root.make_node(context.options.out)
	else:
	    buildNode = context.bldnode
	if context.options.destdir != "":
	    installNode = context.root.make_node(context.options.destdir)
	else:
	    installNode = context.root.make_node(context.options.prefix)
	buildLib = buildNode.make_node('lib')
	buildInclude = buildNode.make_node('include')
	return cls(OutputTree.fromNode(buildNode, buildNode.make_node('bin'),
		buildLib, buildNode.make_node('doc'),
		buildInclude, buildNode.make_node('test')),
		OutputTree.fromNode(installNode, installNode.make_node('bin'),
		installNode.make_node('lib'), installNode.make_node('doc'),
		installNode.make_node('include'), installNode.make_node('test')),
		[buildInclude.abspath()], [buildLib.abspath()], dict())

    def __repr__(self):
	return "__import__('waflib').extras.layout.%s(%s, %s, %s, %s, %s)" % (
		self.__class__.__name__, self.build_tree.__repr__(),
		self.install_tree.__repr__(), self.include_path_list.__repr__(),
		self.lib_path_list.__repr__(), self.__product_set.__repr__())

    def addProduct(self, product):
	self.__product_set[product.name] = product

    def getProduct(self, name):
	return self.__product_set[name]
