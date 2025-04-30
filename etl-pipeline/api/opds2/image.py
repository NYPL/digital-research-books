from .link import Link


class Image(Link):
    def __repr__(self):
        return '<Image(href="{}", type="{}")>'.format(self.href, self.type)
