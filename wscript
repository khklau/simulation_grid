from waflib.extras.preparation import PreparationContext
from waflib.extras.build_status import BuildStatus

def options(optCtx):
    optCtx.recurse('env')
    optCtx.recurse('dep')
    optCtx.recurse('main')

def prepare(prepCtx):
    prepCtx.recurse('env')
    prepCtx.recurse('dep')
    prepCtx.recurse('main')

def configure(confCtx):
    confCtx.recurse('env')
    confCtx.recurse('dep')
    confCtx.recurse('main')

def build(buildCtx):
    status = BuildStatus.init(buildCtx.path.abspath())
    buildCtx.recurse('dep')
    buildCtx.recurse('main')
    status.setSuccess()

def install(installCtx):
    installCtx.recurse('main')
