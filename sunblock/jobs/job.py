from zenlog import log

class Job(object):

    def __init__(self):
        self.order = 0
        self.config = {}

    #TODO Switch to OrderedDict
    def add_key(self, name, desc, prompt, type, validate=None):
        self.config[name] = {
            "key": name,
            "desc": desc,
            "prompt": prompt,
            "type": type,
            "value": None,
            "order": self.order,
            "validate": validate
        }
        self.order += 1

    def check(self):
        for key, conf in self.config.items():
            if conf["value"] is None:
                return False
        return True

    def set_key(self, key, value):
        if key not in self.config:
            log.warn("Attempted to set invalid key: '%s'" % key)
            return

        if self.config[key]["validate"] is not None:
            if not self.config[key]["validate"](value):
                log.error("'%s' is not a valid value for key '%s'. Aborting." % (str(value), key))
                sys.exit(1)
        self.config[key]["value"] = value

    def execute(self):
        raise NotImplementedError()
