def find_insts(namespace, to_find):
    return {k:v for k, v in namespace.iteritems() if isinstance(v, to_find)}

