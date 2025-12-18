import textwrap


def wrap(s, max_width=80):
    wrapper = textwrap.TextWrapper(
        width=max_width, tabsize=4, replace_whitespace=False, drop_whitespace=True
    )
    return "\n".join([wrapper.fill(l) for l in s.splitlines()])
